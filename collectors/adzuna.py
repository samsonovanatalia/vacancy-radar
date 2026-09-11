"""
Четвёртый коллектор: вакансии из Adzuna (api.adzuna.com).

Чем этот источник принципиально отличается от первых трёх:

  1. Нужен ключ. Adzuna даёт бесплатный доступ после регистрации:
     app_id (кто спрашивает) и app_key (пароль к нему). Оба берём
     из переменных окружения, в коде их нет и не будет.

  2. Это не «отдай мне всё», а поиск. У arbeitnow/remoteok/remotive мы
     забирали общий список и фильтровали потом; здесь нужно САМИМ
     сформулировать запросы. Поэтому вверху файла лежит список запросов:
     четыре фразы × четыре города = 16 обращений к API.

  3. У каждой страны свой раздел: .../jobs/es/search/1 — Испания,
     .../jobs/nl/search/1 — Нидерланды. Город задаётся отдельно,
     параметром where.

Какие поля мы забираем и что они означают — смотри в normalize().

Как запустить (из папки проекта):
    # ключи задать один раз на сессию терминала
    python -m collectors.adzuna
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Шаблон адреса. {country} — двухбуквенный код страны, 1 в конце — номер
# страницы выдачи. Берём только первую страницу: нам нужны самые свежие
# вакансии, а не полный обход рынка (см. параметры запроса ниже).
API_URL = "https://api.adzuna.com/v1/api/jobs/{country}/search/1"

# --- список запросов ---
#
# Держим его двумя списками, а не двенадцатью строчками руками: города и
# фразы меняются независимо друг от друга. Добавить город — одна строка,
# добавить профессию — тоже одна, а не четыре новые пары.
#
# Сколько городов можно себе позволить: бесплатный ключ Adzuna даёт
# 1000 вызовов в месяц. Четыре города × четыре фразы = 16 вызовов за прогон,
# при ежедневном запуске это ~480 в месяц без учёта повторов при сбоях —
# остаётся запас на ручные перезапуски и отладку. Пятый город (20 вызовов,
# ~600 в месяц) ещё помещается, шестой (24, ~730) запас почти съедает.
#
# Страна города должна быть среди тех, что поддерживает API. Ирландии нет:
# на .../jobs/ie/... Adzuna отвечает 404 с UNSUPPORTED_COUNTRY и в той же
# ошибке перечисляет допустимые коды: at, au, be, br, ca, ch, de, es, fr,
# gb, in, it, mx, nl, nz, pl, sg, us, za (проверено вызовом 2026-09-11).
LOCATIONS = [
    ("es", "Barcelona"),
    ("es", "Madrid"),
    ("nl", "Amsterdam"),
    ("de", "Berlin"),
]

PHRASES = [
    "data engineer",
    "analytics engineer",
    "data analyst",
    "business intelligence",
]

# Пауза между запросами, секунды. Двадцать обращений подряд без пауз
# выглядят для чужого сервера как маленькая атака, и по бесплатному ключу
# нас быстро начнут отшивать. Секунда — вежливый минимум.
PAUSE_SECONDS = 1

# Коды временных сбоев: сервер перегружен (503), споткнулся (500, 502, 504)
# или просит сбавить темп (429). Через паузу такой запрос обычно проходит.
# Остальные коды — 400, 401, 403, 404 — постоянные: неверный запрос, ключ
# или адрес. Повтор их не починит, только потратит месячный лимит ключа.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# Паузы перед повторами, секунды. Сколько чисел — столько повторов:
# первая попытка и до трёх повторов, всего до четырёх обращений на запрос.
# Паузы растут: если Adzuna прилегла на минуту, частые повторы подряд
# упрутся в тот же сбой.
RETRY_PAUSES = [5, 15, 45]

# Куда складываем сырой результат. Формат тот же, что у остальных:
# .jsonl — один JSON-объект в каждой строке файла.
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_adzuna.jsonl"


class QueryFailed(Exception):
    """Один запрос к Adzuna не удался: постоянная ошибка или кончились повторы."""


def read_credentials() -> tuple[str, str]:
    """Достаёт ключи из окружения.

    Обращаемся через [], а не .get(): если переменной нет, пусть скрипт
    упадёт сразу и внятно, а не уйдёт в запрос с пустым ключом и вернёт
    непонятную 401.
    """
    try:
        return os.environ["ADZUNA_APP_ID"], os.environ["ADZUNA_APP_KEY"]
    except KeyError as error:
        raise SystemExit(
            f"Не задана переменная окружения {error.args[0]}. "
            "Задайте её в терминале перед запуском."
        ) from error


def fetch_one(country: str, city: str, phrase: str,
              app_id: str, app_key: str) -> list[dict]:
    """Делает запрос к Adzuna и возвращает список найденных вакансий.

    При временном сбое (RETRY_STATUS_CODES или таймаут) повторяет с паузами
    из RETRY_PAUSES. Если запрос так и не удался — бросает QueryFailed.
    """
    params = {
        "app_id": app_id,
        "app_key": app_key,

        # what_and, а не what. what ищет ЛЮБОЕ из слов: по «data engineer»
        # приедет всё, где есть просто «data» — включая data entry и
        # маркетинг. what_and требует, чтобы встретились ВСЕ слова сразу.
        "what_and": phrase,

        # Город. Страна задана в адресе, здесь только населённый пункт.
        "where": city,

        # Сортировка по дате: свежие сверху. Важно потому, что берём
        # одну страницу — значит на ней должны оказаться самые новые,
        # а не самые «релевантные» по мнению Adzuna.
        "sort_by": "date",

        # Не старше трёх дней. Радар показывает сегодняшний рынок;
        # вакансия месячной давности, скорее всего, уже закрыта.
        # Плюс это защита от повторной выкачки одного и того же:
        # ходим каждое утро, окно в три дня перекрывает пропущенный день.
        "max_days_old": 3,

        # 50 — максимум, который Adzuna отдаёт за один запрос.
        "results_per_page": 50,
    }

    # ВАЖНО: не используем raise_for_status() и не печатаем response.url
    # или текст сетевой ошибки — в URL лежит app_key целиком, а он не должен
    # попасть ни в вывод, ни в логи GitHub Actions. Поэтому сообщаем только
    # код ответа или имя класса ошибки. По той же причине QueryFailed бросаем
    # с `from None`: иначе Python приклеит к ней исходную ошибку вместе с URL.
    attempts = len(RETRY_PAUSES) + 1
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(API_URL.format(country=country), params=params, timeout=30)
        except requests.Timeout:
            # Timeout ловит оба случая: не успели соединиться (ConnectTimeout)
            # и соединились, но не дождались ответа (ReadTimeout). Запрос
            # только читает выдачу, повторить его безопасно.
            problem = "таймаут"
        except requests.ConnectionError as error:
            # Соединение оборвалось не по таймауту: DNS, отказ в соединении.
            # В список временных сбоев это не входит — не повторяем.
            raise QueryFailed(f"сетевая ошибка {type(error).__name__}") from None
        else:
            if response.status_code == 200:
                # Берём по [], а не .get(): если структура ответа изменится,
                # пусть скрипт упадёт с понятной ошибкой, а не запишет пустоту.
                return response.json()["results"]
            problem = f"код {response.status_code}"
            if response.status_code not in RETRY_STATUS_CODES:
                raise QueryFailed(f"{problem}, постоянная ошибка, без повторов")

        if attempt == attempts:
            raise QueryFailed(f"{problem}, не помогли {attempts} попытки")
        # attempt начинается с 1, а список пауз — с 0: перед вторым
        # обращением берём RETRY_PAUSES[0] = 5 секунд.
        pause = RETRY_PAUSES[attempt - 1]
        print(f"  {phrase} / {city}: {problem}, попытка {attempt} из {attempts}, "
              f"повтор через {pause} с")
        time.sleep(pause)

    # Сюда не дойдём: последняя попытка либо вернула вакансии, либо бросила
    # QueryFailed. Строка нужна, чтобы функция явно не возвращала None.
    raise AssertionError("недостижимо")


def to_unix(created: str | None) -> int | None:
    """Переводит дату публикации из текста в unix-время (секунды).

    Adzuna отдаёт created строкой вида "2026-09-05T09:37:11Z".
    Буква Z на конце означает UTC. Питон до 3.11 такую запись не понимал,
    поэтому меняем Z на явное "+00:00" — так надёжнее и читается яснее.

    Зачем вообще переводить: у остальных источников в raw лежит unix-время
    целым числом, и в staging все ветки union all должны сойтись по типам.
    """
    if not created:
        return None

    # В try не оборачиваем: если формат даты поменяется, лучше упасть
    # и увидеть это, чем тихо записать null.
    moment = datetime.fromisoformat(created.replace("Z", "+00:00"))
    return int(moment.timestamp())


def normalize(job: dict, query: str) -> dict:
    """Приводит одну вакансию Adzuna к нашему единому виду."""
    # Компания, город и категория приходят вложенными словарями:
    # "company": {"display_name": "..."}. Пишем (job.get(...) or {}),
    # а не job["company"]["display_name"]: у части вакансий блока
    # company нет вовсе, и обращение по ключу уронило бы весь сбор
    # из-за одной кривой строки.
    company = job.get("company") or {}
    location = job.get("location") or {}
    category = job.get("category") or {}

    # Зарплата. Adzuna отдаёт её ДВУХ сортов: настоящую, из объявления,
    # и собственный прогноз («примерно столько платят на такой позиции»).
    # Отличает их флаг salary_is_predicted — причём приходит он строкой
    # "0"/"1", а не true/false, поэтому сравниваем со строкой.
    #
    # Прогноз в вилку не берём: посчитать по нему медиану рынка — значит
    # усреднить чужую модель вместо реальных данных. Сам флаг сохраняем
    # в raw, чтобы решение можно было пересмотреть, не перекачивая всё.
    is_predicted = str(job.get("salary_is_predicted", "0")) == "1"

    return {
        # --- поля источника ---
        "source": "adzuna",
        # id приходит строкой, но str() оставляем как у остальных
        # источников: колонка source_id во всех таблицах raw должна быть
        # string, иначе union all в staging развалится по типам.
        "source_id": str(job["id"]),
        "title": job.get("title"),
        "company_name": company.get("display_name"),
        # display_name — человекочитаемая строка вида
        # "Barcelona, Barcelona Province". Есть ещё location.area списком,
        # но нам для staging нужна одна строка, как у других источников.
        "location": location.get("display_name"),
        # Adzuna не сообщает, удалённая вакансия или нет. Не выдумываем:
        # None здесь означает «источник не знает», а не «не удалённая».
        "remote": None,
        "url": job.get("redirect_url"),
        # Тегов у Adzuna нет вообще. Пустой список, а не None — чтобы поле
        # было того же вида, что у источников с тегами.
        "tags": [],
        # contract_type — "permanent" или "contract". У нас в схеме
        # job_types список, оборачиваем в список из одного элемента.
        "job_types": [job["contract_type"]] if job.get("contract_type") else [],
        "description": job.get("description"),
        "created_at_unix": to_unix(job.get("created")),
        "salary_min": None if is_predicted else job.get("salary_min"),
        "salary_max": None if is_predicted else job.get("salary_max"),
        # --- служебные поля: живут только в raw, в staging не идут ---
        # Флаг прогноза: пригодится, если решим считать статистику
        # отдельно по «настоящим» и «предсказанным» зарплатам.
        "salary_is_predicted": is_predicted,
        # Рубрика Adzuna, например "IT Jobs". Полезно для проверки,
        # что запрос не наловил мусора из других отраслей.
        "category_label": category.get("label"),
        # По какому запросу вакансия приехала. Без этого поля потом
        # не понять, что «analytics engineer» в Мадриде даёт ноль строк.
        "query": query,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def collect() -> tuple[list[dict], dict[str, int], dict[str, str]]:
    """Обходит все запросы.

    Возвращает три вещи: вакансии; сколько вакансий пришло по каждому
    удавшемуся запросу; упавшие запросы с причиной сбоя.
    """
    app_id, app_key = read_credentials()

    result: list[dict] = []
    counts: dict[str, int] = {}
    failed: dict[str, str] = {}

    # Двойной цикл: по каждому городу прогоняем каждую фразу.
    # 4 города × 4 фразы = 16 запросов за один прогон.
    for country, city in LOCATIONS:
        for phrase in PHRASES:
            # Подпись запроса. Кладём её в каждую строку и печатаем в отчёте,
            # так что формат должен быть один и тот же — задаём его здесь.
            query = f"{phrase} / {city}"

            # Один упавший запрос не должен стоить всех остальных:
            # запоминаем сбой и идём дальше. Провал или нет — решаем в конце,
            # когда видно, удалось ли хоть что-то.
            try:
                jobs = fetch_one(country, city, phrase, app_id, app_key)
            except QueryFailed as problem:
                failed[query] = str(problem)
                print(f"  {query}: запрос не удался — {problem}")
            else:
                counts[query] = len(jobs)
                for job in jobs:
                    result.append(normalize(job, query))

            # Пауза после каждого запроса, включая последний: лишняя секунда
            # в конце ничего не стоит, а условие «кроме последнего» — это
            # лишняя ветка в коде ради одной секунды.
            time.sleep(PAUSE_SECONDS)

    return result, counts, failed


def save(rows: list[dict], path: Path = OUTPUT_PATH) -> None:
    """Дописывает строки в файл. Каждая вакансия — отдельная строка JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # "a" значит append — дописать в конец, не стирая то, что было.
    # Дубли нас сейчас не пугают: одна вакансия может найтись сразу по двум
    # фразам, и это нормально — повторы снимает dbt по vacancy_key.
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"записано {len(rows)} строк в {path}")


def report(counts: dict[str, int], failed: dict[str, str]) -> None:
    """Печатает, сколько вакансий пришло по каждому запросу, и итог по сбоям."""
    print("сколько пришло по каждому запросу:")
    for query, amount in counts.items():
        print(f"  {query:<40} {amount}")
    print(f"  {'ИТОГО':<40} {sum(counts.values())}")

    total = len(counts) + len(failed)
    print(f"запросов удалось: {len(counts)} из {total}, упало: {len(failed)}")
    for query, problem in failed.items():
        print(f"  сбой: {query:<40} {problem}")


if __name__ == "__main__":
    rows, counts, failed = collect()
    report(counts, failed)
    # Код возврата решает, пойдёт ли GitHub Actions к следующим шагам.
    # Частичный успех — не провал: пишем то, что получили, и выходим с 0,
    # а сбои уже видны в логе через report. Провал — только если не удался
    # ни один запрос: тогда данных нет совсем, в файл ничего не пишем.
    # SystemExit со строкой печатает её и завершает скрипт с кодом 1.
    if not counts:
        raise SystemExit("Adzuna: не удался ни один запрос, данные не получены.")
    save(rows)
