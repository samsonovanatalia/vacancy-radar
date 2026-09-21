"""
Шестой коллектор: вакансии из Manfred (getmanfred.com).

Manfred — испанская площадка для найма в IT. Не агрегатор: вакансии
размещают сами компании, и карточка у них подробнее, чем где бы то ни было
из проверенного — 8–12 тысяч символов структурированного текста.

Чем этот источник отличается от предыдущих:

  1. Ключ не нужен вообще. Публичный API, проверено 2026-09-21.

  2. У API два уровня, и это главное отличие. Список офферов
     (/api/v2/public/offers) приходит ЦЕЛИКОМ одним запросом — 1651 оффер,
     2.3 МБ, — но описания в нём нет вовсе: только название, компания,
     зарплата, локации и процент удалёнки. Описание живёт в карточке
     (/api/v2/public/offers/{id}), по одному запросу на вакансию.

  3. Фильтра по роли у API нет: ни поиска, ни категорий, ни параметров,
     кроме lang. Поэтому фильтруем на своей стороне — по заголовку из
     списка, ТОЙ ЖЕ регуляркой, что делит роли в витрине. И только за
     подошедшими идём в карточку. Из 1651 оффера дата-ролей ~60, то есть
     мы делаем 60 запросов вместо 1651.

  4. Всё на испанском. Правило языка в витрине (is_english) такие вакансии
     отсеет — это ожидаемо и не ошибка: источник берётся ради испанского
     рынка, а не ради английских вакансий. Смотреть на долю отсеянных надо
     после первой сборки, и если она близка к 100%, источник не нужен.

Какие поля мы забираем и что они означают — смотри в normalize().

Ключей и переменных окружения не нужно.

Как запустить (из папки проекта):
    python -m collectors.manfred          # все дата-роли
    python -m collectors.manfred 5        # пробный прогон: первые 5 карточек
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Список офферов: всё разом, одним запросом.
LIST_URL = "https://www.getmanfred.com/api/v2/public/offers"

# Карточка одного оффера. {id} — числовой id из списка.
DETAIL_URL = "https://www.getmanfred.com/api/v2/public/offers/{offer_id}"

# Ссылка на вакансию для человека. Проверено 2026-09-21: /es/job-offers/
# с id и slug отвечает 200, а /es/job/<slug> — 404.
PAGE_URL = "https://www.getmanfred.com/es/job-offers/{offer_id}/{slug}"

# Язык выдачи. API требует его обязательно: без lang отвечает 400
# «lang must be one of the following values: EN, ES». ES — потому что
# источник испанский, и на ES у офферов заполнено больше полей.
LANG = "ES"

# Роли про данные — ТА ЖЕ регулярка, что в mart_vacancies_scored.sql,
# объединением четырёх её веток (analytics_engineer, product_analyst,
# analyst, data_engineer). Копия правила здесь неприятна, но альтернатива
# хуже: фильтровать источник по одному правилу, а раскладывать по ролям
# в витрине — по другому, и потом не понимать, почему вакансия пришла,
# но роли не получила.
#
# \b с обеих сторон, как в витрине: иначе «big data» и «data engineering
# manager» натащат лишнего.
DATA_ROLE_PATTERN = re.compile(
    r"\b(analytics engineer|bi engineer|bi developer|business intelligence"
    r"|product analyst|product data analyst"
    r"|data analyst|bi analyst"
    r"|data engineer)\b"
)

# Пауза между запросами карточек, секунды. Шестьдесят запросов подряд без
# пауз — это уже заметная нагрузка на чужой бесплатный API. Секунда.
PAUSE_SECONDS = 1

# Коды временных сбоев и паузы перед повторами — как у остальных
# коллекторов. 429 сюда входит: у Manfred при частых запросах карточек
# ответ приходил не JSON-ом, и повтор через паузу помогал.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
RETRY_PAUSES = [5, 15, 45]

# Сколько строк накапливаем перед записью в файл. Прогон идёт минуты
# (60 карточек с паузами), и терять всё из-за сбоя на пятьдесят девятой
# не хочется: пишем частями.
BATCH_SIZE = 20

OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_manfred.jsonl"


class QueryFailed(Exception):
    """Один запрос к Manfred не удался: постоянная ошибка или кончились повторы."""


def fetch_json(url: str, what: str) -> list | dict:
    """Запрашивает адрес и возвращает разобранный JSON.

    При временном сбое повторяет с паузами из RETRY_PAUSES. what — что
    именно запрашиваем, для понятного сообщения в логе.
    """
    attempts = len(RETRY_PAUSES) + 1
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params={"lang": LANG}, timeout=30)
        except requests.Timeout:
            problem = "таймаут"
        except requests.ConnectionError as error:
            raise QueryFailed(f"сетевая ошибка {type(error).__name__}") from None
        else:
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError:
                    # Код 200, а в теле не JSON. У Manfred так отвечает
                    # защита от частых запросов: приходит HTML-страница.
                    # Это временный сбой, а не поломка данных, — повторяем.
                    problem = "ответ не JSON"
            else:
                problem = f"код {response.status_code}"
                if response.status_code not in RETRY_STATUS_CODES:
                    raise QueryFailed(f"{problem}, постоянная ошибка, без повторов")

        if attempt == attempts:
            raise QueryFailed(f"{problem}, не помогли {attempts} попытки")
        pause = RETRY_PAUSES[attempt - 1]
        print(f"  {what}: {problem}, попытка {attempt} из {attempts}, "
              f"повтор через {pause} с", flush=True)
        time.sleep(pause)

    raise AssertionError("недостижимо")


def fetch_offer_list() -> list[dict]:
    """Весь список офферов одним запросом.

    Если этот запрос не удался, продолжать нечем: карточки берутся по id
    из списка. Поэтому здесь QueryFailed не ловим — пусть падает.
    """
    offers = fetch_json(LIST_URL, "список офферов")
    if not isinstance(offers, list):
        raise QueryFailed(f"ожидали список офферов, пришло {type(offers).__name__}")
    return offers


def is_data_role(offer: dict) -> bool:
    """Подходит ли заголовок под роль про данные.

    Заголовок приводим так же, как staging: lower(trim(title)). У Manfred
    в названиях попадается хвостовой пробел («Backend Engineer (PHP) »),
    поэтому trim обязателен.
    """
    return bool(DATA_ROLE_PATTERN.search((offer.get("position") or "").strip().lower()))


# Секции карточки, из которых собираем описание, в порядке чтения.
# Это НЕ все поля карточки: сюда не входят scout (имя и рабочая почта
# рекрутера — чужие персональные данные, нам они не нужны), watches,
# lastStatusChange и картинки-иконки у techs и perks.
DESCRIPTION_SECTIONS = [
    ("introduction",     "Вступление"),
    ("whatWillYouDo",    "Что будешь делать"),
    ("responsibilities", "Обязанности"),
    ("howWillYouDoIt",   "Как"),
    ("whatTheyAskFor",   "Что просят"),
    ("whenWillDoIt",     "Когда"),
    ("whereWillDoIt",    "Где"),
    ("whoWillDoItWith",  "С кем"),
    ("whatOffering",     "Что предлагают"),
    ("inOneMonth",       "Через месяц"),
    ("inThreeMonths",    "Через три месяца"),
    ("inSixMonths",      "Через полгода"),
]

# Картинка, вшитая прямо в текст в виде base64.
#
# Зачем отдельное правило: в одной проверенной карточке поле whereWillDoIt
# занимало 71 933 символа, из которых 71 781 — это одна jpeg-картинка,
# закодированная в base64 внутри markdown. Текста там было 150 символов.
#
# Почему вырезаем, хотя raw мы обычно не чистим: это не текст источника,
# а двоичный файл внутри него. Хранить его в BigQuery — платить за
# мегабайты, которые никто никогда не прочитает, и ломать все измерения
# длины описания. Вместо картинки оставляем отметку с её размером, так
# что факт «здесь была картинка» из raw не пропадает.
BASE64_IMAGE = re.compile(r"data:image/[a-z+]+;base64,[A-Za-z0-9+/=\s]+")


def cut_base64_images(text: str) -> str:
    """Меняет вшитые картинки на отметку с размером."""
    return BASE64_IMAGE.sub(
        lambda m: f"[картинка base64, {len(m.group())} символов вырезано]",
        text,
    )


def build_description(card: dict) -> str | None:
    """Собирает описание из секций карточки.

    Почему собираем сами, а не берём одно поле: у Manfred описания одним
    куском не существует вовсе. Есть двенадцать отдельных полей, и часть
    из них у любой вакансии пустая. Склеиваем непустые, подписывая каждую
    заголовком — иначе в слитном тексте не разобрать, где кончились
    обязанности и начались условия.

    responsibilities приходит списком строк, остальные — строками.
    """
    parts: list[str] = []
    for field, caption in DESCRIPTION_SECTIONS:
        value = card.get(field)
        if isinstance(value, list):
            value = "\n".join(f"* {item}" for item in value if item)
        if not value or not str(value).strip():
            continue
        parts.append(f"## {caption}\n{cut_base64_images(str(value)).strip()}")

    # FAQ идёт последним и отдельно: это пары вопрос-ответ, и там же
    # попадается самое полезное для нашего фильтра — «¿Puedo aplicar si
    # vivo fuera de España?» с ответом.
    faq = card.get("faq") or []
    questions = [f"* {item.get('question')} — {item.get('answer')}"
                 for item in faq if item.get("question")]
    if questions:
        parts.append("## FAQ\n" + "\n".join(questions))

    return "\n\n".join(parts) or None


def to_unix(moment_text: str | None) -> int | None:
    """Переводит время из текста в unix-время (секунды).

    Manfred отдаёт updatedAt в ISO с миллисекундами и Z на конце:
    "2026-09-18T07:48:11.546Z". Z означает UTC; меняем на +00:00, как
    в collectors/adzuna.py.
    """
    if not moment_text:
        return None
    moment = datetime.fromisoformat(moment_text.replace("Z", "+00:00"))
    return int(moment.timestamp())


def positive(value) -> int | None:
    """Ноль и None — это «не указано», а не «ноль евро».

    У Manfred незаполненная зарплата приходит нулём: salaryFrom = 0 стоит
    у 472 офферов из 1651. Записать такой ноль в вилку — значит испортить
    статистику по рынку вакансией с зарплатой 0 €.
    """
    return value if value else None


def normalize(offer: dict, card: dict) -> dict:
    """Приводит одну вакансию Manfred к нашему единому виду.

    На вход идут ОБА уровня: offer из списка и card из карточки. В списке
    лежат зарплата, локации и процент удалёнки, в карточке — описание,
    стек, языки. Порядок и имена полей — как у остальных коллекторов:
    union all в staging сопоставляет колонки по порядку, а не по имени.
    """
    company = offer.get("company") or {}
    locations = offer.get("locations") or []
    working_day = card.get("workingDayInfo") or {}

    # Удалёнка. remotePercentage — число от 0 до 100, а в нашей схеме
    # remote булев. 100 — полностью удалённая, меньше — гибрид или офис.
    # None остаётся None: «источник не знает», а не «не удалённая».
    remote_percentage = offer.get("remotePercentage")
    remote = None if remote_percentage is None else remote_percentage == 100

    # Тип занятости. Полного дня/частичного у Manfred нет отдельным полем,
    # есть workingDayInfo.isFullTime, и отдельно флаг фриланса.
    job_types = []
    if working_day.get("isFullTime") is True:
        job_types.append("full_time")
    elif working_day.get("isFullTime") is False:
        job_types.append("part_time")
    if offer.get("isFreelance"):
        job_types.append("freelance")

    return {
        # --- поля источника ---
        "source": "manfred",
        # id приходит числом. Приводим к строке, как у остальных: колонка
        # source_id во всех таблицах raw должна быть string, иначе union all
        # в staging развалится по типам.
        "source_id": str(offer["id"]),
        # У части названий хвостовой пробел — «Backend Engineer (PHP) ».
        "title": (offer.get("position") or "").strip() or None,
        "company_name": company.get("name"),
        # locations — список строк вида «Vigo, España». Склеиваем в одну
        # строку: в staging у всех источников location строковая. Пустой
        # список у 944 офферов из 1651 — это полностью удалённые вакансии,
        # у них места работы нет; пишем None, а не пустую строку.
        "location": ", ".join(locations) or None,
        "remote": remote,
        "url": PAGE_URL.format(offer_id=offer["id"], slug=offer.get("slug", "")),
        # Стек: techs приходит списком словарей с иконками, берём имена.
        "tags": [t.get("name") for t in (card.get("techs") or []) if t.get("name")],
        "job_types": job_types,
        "description": build_description(card),
        # Даты публикации у Manfred нет — есть только updatedAt. Пишем его,
        # но помним разницу: вакансию могли выложить месяц назад и обновить
        # вчера. Окно в 30 дней в витрине из-за этого будет чуть щедрее.
        "created_at_unix": to_unix(offer.get("updatedAt")),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        # Постраничного обхода нет — список приходит целиком.
        "source_page": 1,
        "salary_min": positive(offer.get("salaryFrom")),
        "salary_max": positive(offer.get("salaryTo")),
        # Зарплаты строкой источник не отдаёт, только числа.
        "salary_text": None,
        # --- служебные поля: живут только в raw, в staging не идут ---
        # Валюта приходит знаком («€»), а не кодом.
        "salary_currency": offer.get("currency"),
        # Процент удалёнки целиком: 40 и 100 — разные вещи, а в булевом
        # remote разница пропадает.
        "remote_percentage": remote_percentage,
        # Требуемые языки с уровнем: [{"code": "EN", "level": "Fluent"}].
        # Пригодится правилу foreign_language_required — у него сейчас
        # единственный источник это ответы модели.
        "languages": [{"code": l.get("code"), "level": l.get("level")}
                      for l in (card.get("languages") or [])],
        # Язык самого объявления: ["ES"], ["EN"] или оба.
        "offer_languages": offer.get("offerLanguages") or [],
        "slug": offer.get("slug"),
    }


def save(rows: list[dict], path: Path = OUTPUT_PATH) -> None:
    """Дописывает строки в файл. Каждая вакансия — отдельная строка JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    print(f"  записано {len(rows)} строк в {path.name}", flush=True)


def collect(limit: int | None = None,
            path: Path = OUTPUT_PATH) -> tuple[list[dict], dict]:
    """Забирает список, отбирает дата-роли, тянет карточки.

    Возвращает собранные строки и итоги прогона. Пишет частями по
    BATCH_SIZE: прогон идёт минуты, и сбой на последней карточке не должен
    стоить всех предыдущих.
    """
    offers = fetch_offer_list()
    print(f"офферов в списке: {len(offers)}", flush=True)

    candidates = [o for o in offers if is_data_role(o)]
    print(f"из них дата-ролей по заголовку: {len(candidates)}", flush=True)

    if limit is not None:
        candidates = candidates[:limit]
        print(f"пробный прогон: берём первые {len(candidates)}", flush=True)

    collected: list[dict] = []
    batch: list[dict] = []
    failed: dict[str, str] = {}

    for number, offer in enumerate(candidates, start=1):
        title = (offer.get("position") or "").strip()
        # Сбой на одной карточке не должен стоить остальных: запоминаем
        # и идём дальше. Провал или нет — решаем в конце, когда видно,
        # удалось ли хоть что-то.
        try:
            card = fetch_json(DETAIL_URL.format(offer_id=offer["id"]),
                              f"карточка {offer['id']}")
        except QueryFailed as problem:
            failed[str(offer["id"])] = str(problem)
            print(f"  {number}/{len(candidates)} {title[:40]}: "
                  f"карточка не получена — {problem}", flush=True)
        else:
            row = normalize(offer, card)
            batch.append(row)
            collected.append(row)
            print(f"  {number}/{len(candidates)} {title[:40]}: "
                  f"текст {len(row['description'] or '')} символов", flush=True)

            if len(batch) == BATCH_SIZE:
                save(batch, path)
                batch = []

        time.sleep(PAUSE_SECONDS)

    if batch:
        save(batch, path)

    return collected, {"offers_total": len(offers),
                       "candidates": len(candidates),
                       "failed": failed}


def report(totals: dict) -> None:
    """Итог прогона: сколько было, сколько взяли, что упало."""
    failed = totals["failed"]
    ok = totals["candidates"] - len(failed)
    print(f"\nитог: офферов {totals['offers_total']}, "
          f"дата-ролей {totals['candidates']}, "
          f"карточек получено {ok}, сбоев {len(failed)}")
    for offer_id, problem in failed.items():
        print(f"  сбой: оффер {offer_id} — {problem}")


def measure(rows: list[dict], totals: dict) -> None:
    """Измерения по собранному — печатаются после каждого прогона."""
    if not rows:
        print("измерять нечего: не собрано ни одной вакансии")
        return

    print(f"\nизмерения по {len(rows)} собранным вакансиям:")

    # 1. Длина описания. Медиана, а не среднее: одна вакансия-простыня
    # утащила бы среднее вверх и спрятала короткие.
    lengths = sorted(len(row["description"] or "") for row in rows)
    print(f"  описание: медиана {statistics.median(lengths):.0f}, "
          f"максимум {lengths[-1]}, минимум {lengths[0]}")
    empty = sum(1 for length in lengths if length == 0)
    if empty:
        print(f"  ВНИМАНИЕ: без описания {empty} вакансий")

    # 2. Доля дата-ролей. Внутри собранного она обязана быть 100%: мы
    # сами так отобрали. Печатаем как проверку фильтра — если тут не 100%,
    # значит регулярка разошлась с отбором. И отдельно долю от всего
    # списка: это уже про источник, а не про фильтр.
    data_roles = sum(1 for row in rows
                     if DATA_ROLE_PATTERN.search((row["title"] or "").strip().lower()))
    print(f"  заголовок — роль про данные: {data_roles} из {len(rows)} "
          f"({data_roles / len(rows):.0%}); "
          f"во всём списке {totals['candidates']} из {totals['offers_total']} "
          f"({totals['candidates'] / totals['offers_total']:.0%})")

    # 3. Зарплата. Источник отдаёт числа, а не текст, — считаем, у скольких
    # вилка вообще есть.
    with_salary = sum(1 for row in rows if row["salary_min"] or row["salary_max"])
    print(f"  зарплатная вилка указана: {with_salary} из {len(rows)} "
          f"({with_salary / len(rows):.0%})")
    if with_salary:
        mins = sorted(row["salary_min"] for row in rows if row["salary_min"])
        maxs = sorted(row["salary_max"] for row in rows if row["salary_max"])
        if mins and maxs:
            print(f"    медиана вилки: от {statistics.median(mins):.0f} "
                  f"до {statistics.median(maxs):.0f} "
                  f"{rows[0]['salary_currency'] or ''}")

    # 4. Удалёнка. Булев remote прячет разницу между 40% и 100%, поэтому
    # смотрим на проценты.
    percentages = [row["remote_percentage"] for row in rows
                   if row["remote_percentage"] is not None]
    if percentages:
        full = sum(1 for p in percentages if p == 100)
        none_remote = sum(1 for p in percentages if p == 0)
        print(f"  удалёнка: 100% у {full} вакансий "
              f"({full / len(rows):.0%}), 0% у {none_remote}, "
              f"медиана {statistics.median(percentages):.0f}%")

    # 5. Дубли внутри прогона. У Manfred id уникален, поэтому дублей здесь
    # быть не должно: число важно как проверка, что список не пришёл
    # с повторами.
    unique = len({row["source_id"] for row in rows})
    print(f"  уникальных по source_id: {unique}, "
          f"дублей внутри прогона: {len(rows) - unique}")

    # 6. Язык объявления. Источник испанский, и это решает судьбу
    # источника: вакансии на испанском отсеет правило is_english в витрине.
    spanish = sum(1 for row in rows if row["offer_languages"] == ["ES"])
    english = sum(1 for row in rows if "EN" in (row["offer_languages"] or []))
    print(f"  язык объявления: только ES у {spanish}, с EN у {english}")


def read_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Сбор вакансий из Manfred.")
    parser.add_argument(
        "limit", nargs="?", type=int, default=None,
        help="сколько карточек взять (для пробного прогона); без числа — все",
    )
    args = parser.parse_args()
    if args.limit is not None and args.limit < 1:
        parser.error("число должно быть больше нуля")
    return args


if __name__ == "__main__":
    arguments = read_args()
    rows, totals = collect(arguments.limit)
    report(totals)

    # Частичный успех — не провал: что собрали, то уже записано частями.
    # Провал — только если не удалось получить ни одной карточки.
    if not rows:
        raise SystemExit("Manfred: не получено ни одной карточки, данных нет.")

    measure(rows, totals)
