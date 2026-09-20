"""
Пятый коллектор: вакансии из Careerjet (search.api.careerjet.net/v4/query).

Careerjet — агрегатор: он сам не публикует вакансии, а собирает их с сайтов
работодателей и других досок. Для нас это значит две вещи: охват по Испании
больше, чем у Adzuna, и та же вакансия может прийти и отсюда, и оттуда.
Дубли снимает dbt по vacancy_key, здесь мы их не боимся.

Чем этот источник отличается от Adzuna:

  1. Ключ обязателен, и это именно v4. Есть ещё старый адрес
     public.api.careerjet.net/search — он отвечает без ключа, но только
     на демо-референты из их примеров кода (example.com, localhost).
     На любой свой домен он даёт 401 «The legacy Job Search API is only
     accessible for authenticated legacy users. Please use the new API (v4)»
     — проверено 2026-09-20. Подменять чужой референт мы не будем, поэтому
     работаем по v4 с ключом Publisher-аккаунта.

  2. Ключ передаётся не параметром, а HTTP Basic: ключ — имя пользователя,
     пароль пустой. Поэтому в URL его нет, и адрес можно спокойно печатать
     в лог — в отличие от Adzuna, где app_key лежит прямо в строке запроса.

  3. Полный текст описания приходит сразу, без догрузки страниц. Ценой
     одного параметра — fragment_size, см. ниже. Это главная причина, по
     которой источник вообще взят: у Adzuna описание обрезано на 500
     символах, и ради полного текста пришлось писать fetch/adzuna_pages.py.

  4. У вакансии нет id. Его приходится считать самим — см. make_source_id().

Какие поля мы забираем и что они означают — смотри в normalize().

Перед запуском:
    export CAREERJET_API_KEY=ключ-из-Publisher-аккаунта

Как запустить (из папки проекта):
    python -m collectors.careerjet
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

# Адрес v4. Старый public.api.careerjet.net/search здесь не используется
# намеренно — причина в шапке файла.
API_URL = "https://search.api.careerjet.net/v4/query"

# --- список запросов ---
#
# Фразы те же четыре, что у Adzuna: два коллектора ищут одно и то же, и
# разойтись списки не должны — иначе разницу в выдаче не объяснить
# источником, она объяснится разными запросами.
PHRASES = [
    "data engineer",
    "analytics engineer",
    "data analyst",
    "business intelligence",
]

# Где ищем. None означает «по всей стране»: документация v4 просит в этом
# случае параметр location не передавать вовсе, а не слать пустую строку.
# Страна задаётся не здесь, а через LOCALE — см. ниже.
#
# Третий вход с None не лишний рядом с двумя городами: вакансия в Валенсии
# или полностью удалённая по Испании ни в Барселону, ни в Мадрид не попадёт.
# Дубли с городами неизбежны и ожидаемы — их снимает dbt.
LOCATIONS = [
    "Barcelona",
    "Madrid",
    None,
]

# Язык и страна выдачи. es_ES — Испания. Задаёт и рынок, по которому ищем,
# и язык интерфейсных строк в ответе.
#
# Почему фразы английские при испанской локали: испаноязычные вакансии
# всё равно отсеются правилом языка в mart_vacancies_scored (is_english
# по доле английских маркеров). Искать «analista de datos», чтобы потом
# выбросить найденное, — потраченные запросы.
LOCALE = "es_ES"

# ВАЖНЕЙШИЙ ПАРАМЕТР ФАЙЛА.
#
# По умолчанию Careerjet отдаёт fragment_size=120 — не описание, а огрызок
# вокруг ключевых слов. Замерено 2026-09-20 на 60 вакансиях: медиана 243
# символа, максимум 267, и ВСЕ 60 из 60 заканчивались многоточием.
# Это ровно та беда, из-за которой у Adzuna пришлось писать отдельный
# скрипт догрузки страниц (fetch/adzuna_pages.py): API отдаёт 500 символов,
# а полный текст лежит только на сайте.
#
# С fragment_size=20000 тот же запрос отдаёт медиану 3338 и максимум 5612,
# текст начинается с начала объявления и кончается его концом. Проверено,
# что это потолок, а не просто «больше»: на 100000 цифры не меняются
# (медиана 3338, максимум 5612) — значит упёрлись в длину самих объявлений,
# а не в лимит параметра.
#
# Вывод: без этого параметра источник теряет смысл. Мы берём Careerjet
# именно ради полного текста в одном ответе; с обрезкой он хуже Adzuna.
# Если параметр когда-нибудь перестанут поддерживать, это будет видно
# по измерениям в measure() — и тогда либо догрузка страниц, либо отказ.
FRAGMENT_SIZE = 20000

# Сколько вакансий за один запрос. 100 — максимум, который разрешает v4.
PAGE_SIZE = 100

# Сортировка: свежие сверху. Как и у Adzuna, берём одну страницу, поэтому
# на ней должны оказаться самые новые, а не самые «релевантные».
SORT = "date"

# Пауза между запросами, секунды. Двенадцать обращений подряд без пауз
# выглядят для чужого сервера как маленькая атака. Секунда — вежливый минимум.
PAUSE_SECONDS = 1

# Коды временных сбоев — те же, что у Adzuna: сервер перегружен (503),
# споткнулся (500, 502, 504) или просит сбавить темп (429). Остальные —
# 400, 401, 403, 404 — постоянные: неверный запрос, ключ или адрес.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# Паузы перед повторами, секунды. Сколько чисел — столько повторов.
RETRY_PAUSES = [5, 15, 45]

# Куда складываем сырой результат, в одном формате с остальными: .jsonl —
# один JSON-объект в каждой строке файла.
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_careerjet.jsonl"


class QueryFailed(Exception):
    """Один запрос к Careerjet не удался: постоянная ошибка или кончились повторы."""


def read_api_key() -> str:
    """Достаёт ключ из окружения.

    Через [], а не .get(): если переменной нет, пусть скрипт упадёт сразу
    и внятно, а не уйдёт в запрос с пустым ключом и вернёт непонятную 401.
    """
    try:
        return os.environ["CAREERJET_API_KEY"]
    except KeyError as error:
        raise SystemExit(
            f"Не задана переменная окружения {error.args[0]}. "
            "Ключ берётся в Publisher-аккаунте на careerjet.com/partners "
            "и задаётся в терминале перед запуском."
        ) from error


def build_params(phrase: str, location: str | None) -> dict:
    """Собирает параметры одного запроса.

    Вынесено в отдельную функцию, чтобы тест мог проверить параметры,
    не трогая сеть: fragment_size тут слишком важен, чтобы проверять его
    глазами.
    """
    params = {
        "locale_code": LOCALE,
        "keywords": phrase,
        "fragment_size": FRAGMENT_SIZE,
        "page_size": PAGE_SIZE,
        "sort": SORT,

        # user_ip и user_agent v4 требует обязательно: API рассчитан на
        # сайт, который показывает вакансии живому посетителю, и эти два
        # поля описывают ЕГО, а не нас. Посетителя у нас нет — мы робот,
        # который раз в сутки складывает выдачу в файл.
        #
        # Поэтому не выдумываем правдоподобный чужой адрес и не подставляем
        # свой: 203.0.113.0/24 — диапазон TEST-NET-3 из RFC 5737, он
        # зарезервирован для документации и примеров и никому не принадлежит.
        # Это честное «здесь нет конечного пользователя».
        "user_ip": "203.0.113.5",

        # А вот user_agent честный: пусть по логам видно, кто пришёл.
        "user_agent": "vacancy-radar/0.1 (+https://github.com/vacancy-radar)",
    }

    # location передаём, только если он задан: для поиска по всей стране
    # документация просит параметр опустить, а не слать пустую строку.
    if location is not None:
        params["location"] = location

    return params


def fetch_one(phrase: str, location: str | None, api_key: str) -> list[dict]:
    """Делает запрос к Careerjet и возвращает список найденных вакансий.

    При временном сбое (RETRY_STATUS_CODES или таймаут) повторяет с паузами
    из RETRY_PAUSES. Если запрос так и не удался — бросает QueryFailed.
    """
    params = build_params(phrase, location)

    # Ключ идёт HTTP Basic: имя пользователя — ключ, пароль пустой.
    # requests сам закодирует это в заголовок Authorization, так что в URL
    # ключ не попадёт и в логах не окажется.
    auth = (api_key, "")

    attempts = len(RETRY_PAUSES) + 1
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(API_URL, params=params, auth=auth, timeout=30)
        except requests.Timeout:
            # Timeout ловит оба случая: не успели соединиться и соединились,
            # но не дождались ответа. Запрос только читает выдачу,
            # повторить его безопасно.
            problem = "таймаут"
        except requests.ConnectionError as error:
            # Соединение оборвалось не по таймауту: DNS, отказ в соединении.
            # Во временные сбои это не входит — не повторяем.
            raise QueryFailed(f"сетевая ошибка {type(error).__name__}") from None
        else:
            if response.status_code == 200:
                body = response.json()

                # Даже с кодом 200 Careerjet может вернуть не выдачу, а
                # ошибку: {"type": "ERROR", "error": "..."}. Молча отдать
                # пустой список здесь нельзя — получилось бы «источник
                # ничего не нашёл» вместо «запрос не удался».
                if body.get("type") == "ERROR":
                    raise QueryFailed(f"ответ с ошибкой: {body.get('error')}")

                # По [], а не .get(): если структура ответа изменится,
                # пусть скрипт упадёт с понятной ошибкой, а не запишет пустоту.
                return body["jobs"]

            problem = f"код {response.status_code}"
            if response.status_code not in RETRY_STATUS_CODES:
                raise QueryFailed(f"{problem}, постоянная ошибка, без повторов")

        if attempt == attempts:
            raise QueryFailed(f"{problem}, не помогли {attempts} попытки")

        # attempt начинается с 1, а список пауз — с 0: перед вторым
        # обращением берём RETRY_PAUSES[0] = 5 секунд.
        pause = RETRY_PAUSES[attempt - 1]
        print(f"  {phrase} / {location or 'вся Испания'}: {problem}, "
              f"попытка {attempt} из {attempts}, повтор через {pause} с")
        time.sleep(pause)

    # Сюда не дойдём: последняя попытка либо вернула вакансии, либо бросила
    # QueryFailed. Строка нужна, чтобы функция явно не возвращала None.
    raise AssertionError("недостижимо")


def to_unix(date_text: str | None) -> int | None:
    """Переводит дату публикации из текста в unix-время (секунды).

    Careerjet отдаёт date в формате RFC 2822 — так пишут дату в заголовках
    писем: "Wed, 12 Aug 2026 07:55:03 GMT". Adzuna отдавала ISO, поэтому
    там хватало fromisoformat, а здесь нужен разборщик почтового формата
    из стандартной библиотеки.

    Зачем переводить: у остальных источников в raw лежит unix-время целым
    числом, и в staging все ветки union all должны сойтись по типам.
    """
    if not date_text:
        return None

    # В try не оборачиваем: если формат даты поменяется, лучше упасть
    # и увидеть это, чем тихо записать null.
    moment = parsedate_to_datetime(date_text)

    # parsedate_to_datetime возвращает «наивное» время, если в строке не
    # было зоны. У Careerjet зона всегда есть (GMT), но подстрахуемся:
    # у наивного времени .timestamp() считает по часам МАШИНЫ, а на
    # раннере GitHub они UTC, у меня — Мадрид. Разница в два часа тихо
    # уехала бы в данные.
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)

    return int(moment.timestamp())


def make_source_id(job: dict) -> str:
    """Считает устойчивый id вакансии: у Careerjet своего id нет.

    В ответе v4 есть title, company, date, locations, salary, description
    и url — поля идентификатора нет вовсе. Взять нечего, приходится считать.

    Почему не url: он ведёт на jobviewtrack.com со случайным на вид токеном.
    Что в этом токене — наш ключ, сессия, время запроса — снаружи не видно,
    и устойчивость его между прогонами не проверить. Идентификатор, который
    может молча меняться каждое утро, хуже, чем никакого: в raw поедут
    дубли, а dbt их не склеит.

    Поэтому берём то, что у вакансии не меняется: название, компания, город.
    Дату НЕ берём намеренно — агрегаторы переставляют её при переиндексации,
    и вакансия каждый день считалась бы новой.

    Цена решения: две РАЗНЫЕ вакансии с одинаковым названием в одной
    компании и одном городе сольются в одну. Для подборки это скорее
    правильно — человеку дважды одно и то же показывать незачем.
    """
    # Пустые поля заменяем на "", а не пропускаем: иначе вакансия без
    # компании и вакансия без города дали бы одну и ту же склейку.
    parts = [
        job.get("title") or "",
        job.get("company") or "",
        job.get("locations") or "",
    ]

    # \x1f — «разделитель полей» из ASCII. Обычный дефис или пробел в
    # названиях встречается, и тогда "a-b" + "c" совпало бы с "a" + "b-c".
    joined = "\x1f".join(parts)
    return hashlib.sha1(joined.encode("utf-8")).hexdigest()


def normalize(job: dict, query: str) -> dict:
    """Приводит одну вакансию Careerjet к нашему единому виду.

    Порядок и имена полей — как у остальных коллекторов: stg_vacancies
    склеивает источники через union all, а он сопоставляет колонки
    ПО ПОРЯДКУ, а не по имени.
    """
    # Зарплата. Проверено 2026-09-20: признака «это наша оценка» у Careerjet
    # нет вообще. Единственное похожее поле — salary_type, но это не флаг
    # прогноза, а ПЕРИОД: Y — в год, M — в месяц, W — в неделю, D — в день,
    # H — в час (подтверждено документацией v4).
    #
    # Проверяли и по данным: из 20 вакансий вилка была у 8, и у 6 из них
    # ровно эта цифра нашлась в тексте объявления. Два промаха объясняются
    # тем, что description — склейка фрагментов, и строка с зарплатой могла
    # в неё не попасть.
    #
    # Поэтому, в отличие от Adzuna с её salary_is_predicted, здесь НИЧЕГО
    # не обнуляем: цифры взяты из объявления.
    #
    # Но период сохраняем отдельным полем и в marts без него не считаем:
    # 30000 в год и 30000 в месяц — не одно и то же, а лежат они в одной
    # колонке salary_min.
    return {
        # --- поля источника ---
        "source": "careerjet",
        "source_id": make_source_id(job),
        "title": job.get("title"),
        "company_name": job.get("company"),
        # locations приходит одной строкой ("Madrid", "Barcelona, Cataluña"),
        # а не списком, несмотря на множественное число в названии.
        "location": job.get("locations"),
        # Careerjet не сообщает, удалённая вакансия или нет. Не выдумываем:
        # None здесь означает «источник не знает», а не «не удалённая».
        # Так же сделано у Adzuna.
        "remote": None,
        "url": job.get("url"),
        # Тегов у Careerjet нет. Пустой список, а не None — чтобы поле было
        # того же вида, что у источников с тегами.
        "tags": [],
        # Тип контракта в ответе не приходит: он есть только как ФИЛЬТР
        # запроса (contract_type), а обратно в выдаче не отдаётся.
        "job_types": [],
        # Описание пишем как есть, с разметкой. Careerjet оборачивает
        # найденные слова в <b>: «Data <b>Analyst</b>». Это разметка
        # источника, и чистить её здесь мы не будем — raw хранит то, что
        # пришло. Снимает её dbt тем же clean_html_text, что и у остальных.
        "description": job.get("description"),
        "created_at_unix": to_unix(job.get("date")),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        # Постраничного обхода у нас нет — берём одну страницу, как у Adzuna.
        "source_page": 1,
        "salary_min": job.get("salary_min"),
        "salary_max": job.get("salary_max"),
        # Зарплата строкой, как её собрал Careerjet:
        # "&euro;33000 - 36000 per year". Разбирать текст в числа здесь
        # не будем — это бизнес-логика для marts. Заодно это страховка:
        # если решение по salary_min окажется неверным, перепроверить
        # можно по raw, не перекачивая источник.
        "salary_text": job.get("salary") or None,
        # --- служебные поля: живут только в raw, в staging не идут ---
        # Период зарплаты (Y/M/W/D/H) — без него цифры выше несравнимы.
        "salary_period": job.get("salary_type"),
        # Валюта: "EUR". Отдельной колонки salary_currency в staging нет,
        # но в raw кладём — рынок испанский, а вакансии попадаются
        # британские и швейцарские.
        "salary_currency": job.get("salary_currency_code"),
        # По какому запросу вакансия приехала. Без этого поля потом не
        # понять, что «analytics engineer» по Мадриду даёт ноль строк.
        "query": query,
    }


def collect() -> tuple[list[dict], dict[str, int], dict[str, str]]:
    """Обходит все запросы.

    Возвращает три вещи: вакансии; сколько вакансий пришло по каждому
    удавшемуся запросу; упавшие запросы с причиной сбоя.
    """
    api_key = read_api_key()

    result: list[dict] = []
    counts: dict[str, int] = {}
    failed: dict[str, str] = {}

    # Двойной цикл: по каждому месту прогоняем каждую фразу.
    # 3 места × 4 фразы = 12 запросов за один прогон.
    for location in LOCATIONS:
        for phrase in PHRASES:
            # Подпись запроса. Кладём её в каждую строку и печатаем в отчёте,
            # так что формат должен быть один и тот же — задаём его здесь.
            query = f"{phrase} / {location or 'вся Испания'}"

            # Один упавший запрос не должен стоить всех остальных:
            # запоминаем сбой и идём дальше. Провал или нет — решаем в конце,
            # когда видно, удалось ли хоть что-то.
            try:
                jobs = fetch_one(phrase, location, api_key)
            except QueryFailed as problem:
                failed[query] = str(problem)
                print(f"  {query}: запрос не удался — {problem}", flush=True)
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
    # Дубли нас не пугают: одна вакансия находится и по «data analyst»,
    # и по «business intelligence» — повторы снимает dbt по vacancy_key.
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


# --- измерения ---
#
# Печатаются при каждом прогоне, а не лежат отдельным скриптом. Причина:
# всё, ради чего взят этот источник, может тихо сломаться. Перестанет
# работать fragment_size — описания снова станут по 250 символов с
# многоточием, и это должно быть видно в логе в тот же день, а не через
# месяц по жалобе на куцый дайджест.

# Роли про данные — ТА ЖЕ регулярка, что в mart_vacancies_scored.sql,
# объединением четырёх её веток (analytics_engineer, product_analyst,
# analyst, data_engineer). Держать её здесь копией неприятно, но
# альтернатива — считать долю по другому правилу, чем витрина, и потом
# спорить с собственными числами.
#
# \b с обеих сторон, как в витрине: иначе «data engineering manager»
# и «big data» насчитают лишнего.
DATA_ROLE_PATTERN = re.compile(
    r"\b(analytics engineer|bi engineer|bi developer|business intelligence"
    r"|product analyst|product data analyst"
    r"|data analyst|bi analyst"
    r"|data engineer)\b"
)


def text_length(description: str | None) -> int:
    """Длина описания БЕЗ разметки: столько текста реально получит человек.

    Careerjet оборачивает найденные слова в <b>, и считать их как текст —
    значит завысить длину на несколько десятков символов на каждой вакансии.
    """
    if not description:
        return 0
    return len(re.sub(r"<[^>]+>", "", description))


def measure(rows: list[dict]) -> None:
    """Печатает измерения по собранному: длина описаний, доля дата-ролей, дубли."""
    if not rows:
        print("измерять нечего: не собрано ни одной вакансии")
        return

    print(f"\nизмерения по {len(rows)} собранным вакансиям:")

    # 1. Длина описания. Медиана, а не среднее: одна вакансия-простыня
    # на 5000 символов утащила бы среднее вверх и спрятала обрезку.
    lengths = sorted(text_length(row["description"]) for row in rows)
    median = lengths[len(lengths) // 2]
    print(f"  описание: медиана {median}, максимум {lengths[-1]}, "
          f"минимум {lengths[0]}")

    # 2. Сколько обрезано. Главная проверка файла: при работающем
    # fragment_size многоточий почти нет, при сломанном — почти все.
    cut = sum(1 for row in rows
              if re.sub(r"<[^>]+>", "", row["description"] or "").rstrip().endswith(("...", "…")))
    print(f"  заканчивается многоточием: {cut} из {len(rows)} "
          f"({cut / len(rows):.0%})")
    if cut > len(rows) / 2:
        print("  ВНИМАНИЕ: больше половины описаний обрезано — "
              "проверьте, работает ли fragment_size")

    # 3. Доля дата-ролей. Заголовок приводим так же, как staging:
    # title_normalized = lower(trim(title)).
    data_roles = sum(1 for row in rows
                     if DATA_ROLE_PATTERN.search((row["title"] or "").strip().lower()))
    print(f"  заголовок — роль про данные: {data_roles} из {len(rows)} "
          f"({data_roles / len(rows):.0%})")

    # 4. Дубли внутри прогона. Это НЕ ошибка: одна вакансия находится
    # и по «data analyst» в Барселоне, и по той же фразе по всей Испании.
    # Число важно как оценка — сколько из собранного доедет до витрины
    # после склейки по vacancy_key.
    unique = len({row["source_id"] for row in rows})
    print(f"  уникальных по source_id: {unique}, "
          f"дублей внутри прогона: {len(rows) - unique}")


if __name__ == "__main__":
    rows, counts, failed = collect()
    report(counts, failed)

    # Код возврата решает, пойдёт ли GitHub Actions к следующим шагам.
    # Частичный успех — не провал: пишем то, что получили, и выходим с 0,
    # а сбои уже видны в логе через report. Провал — только если не удался
    # ни один запрос: тогда данных нет совсем, в файл ничего не пишем.
    if not counts:
        raise SystemExit("Careerjet: не удался ни один запрос, данные не получены.")

    measure(rows)
    save(rows)
