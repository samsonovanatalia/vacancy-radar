"""
Третий коллектор: вакансии с сайта remotive.com.

Что делает этот файл, по-человечески:
  1. Ходит по адресу https://remotive.com/api/remote-jobs и просит отдать
     список вакансий. Регистрации и ключей не нужно, как и у первых двух.
  2. Получает JSON вида {"job-count": 17, "jobs": [ {вакансия}, ... ]}.
     Обратите внимание: у RemoteOK ответ был просто списком, а здесь —
     словарь, и список лежит внутри по ключу "jobs". Каждый источник
     отдаёт по-своему, и разгребать это — работа коллектора.
  3. Оставляет нужные поля и приводит их к нашему единому виду.
  4. Дописывает результат в data/raw_remotive.jsonl — по вакансии в строке.

Как запустить (в терминале, из папки проекта):
    python -m collectors.remotive

Чем этот источник отличается от предыдущих:
  - job_type приходит ОДНОЙ строкой ("full_time"), а у нас в схеме
    job_types — список. Оборачиваем в список из одного элемента.
  - Зарплата приходит свободным текстом ("$50-$75 /hour", "$20k -$35k").
    Разобрать её в два числа честно нельзя — непонятно, в час это или в год.
    Поэтому кладём текст как есть в salary_text, а salary_min/max = None.
    Правило проекта: чисел не выдумываем.
  - Дата публикации — текст "2026-09-05T09:37:11". У остальных источников
    в raw лежит unix-время (целое число секунд), поэтому переводим.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import requests

# Адрес источника. Открытый, ключ не нужен.
#
# Про category=data: по документации это фильтр по категории («Data and
# Analytics»). На деле API его сейчас ИГНОРИРУЕТ — с любым значением
# параметра и вовсе без него приходит один и тот же список из всех
# категорий. Параметр оставлен намеренно: он не мешает, и если Remotive
# починит фильтр, мы получим то, что просили, без правки кода.
# Фильтровать по категории на своей стороне не стали: сегодня в выдаче
# ни одной вакансии из «Data and Analytics» нет, и мы получили бы ноль строк.
API_URL = "https://remotive.com/api/remote-jobs?category=data"

# Куда складываем сырой результат. Формат тот же, что у остальных:
# .jsonl — один JSON-объект в каждой строке файла.
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_remotive.jsonl"


def fetch_all() -> list[dict]:
    """Забирает весь список вакансий одним запросом."""
    response = requests.get(API_URL, timeout=30,
                            headers={"User-Agent": "vacancy-radar/0.1 (learning project)"})
    response.raise_for_status()
    payload = response.json()

    # Список вакансий лежит внутри словаря. Берём по [], а не .get():
    # если структура ответа изменится, пусть скрипт упадёт с понятной
    # ошибкой, а не запишет пустой файл. Ошибки не глотаем.
    items = payload["jobs"]

    jobs = []
    for item in items:
        if item.get("id"):
            jobs.append(item)

    print(f"получено элементов: {len(items)}, из них вакансий: {len(jobs)}")
    return jobs


def to_unix(published: str | None) -> int | None:
    """Переводит дату публикации из текста в unix-время (секунды).

    Remotive отдаёт "2026-09-05T09:37:11" — ISO-формат без указания
    таймзоны. Питон в таком случае считает время «наивным», и .timestamp()
    у него посчитался бы по таймзоне машины: локально одно число, на
    раннере GitHub — другое. Поэтому явно говорим «это UTC» через
    .replace(tzinfo=...) — Remotive отдаёт время именно в UTC.
    """
    if not published:
        return None

    # fromisoformat специально не оборачиваем в try: если формат даты
    # вдруг поменяется, лучше упасть и увидеть это, чем тихо записать null.
    moment = datetime.fromisoformat(published).replace(tzinfo=timezone.utc)
    return int(moment.timestamp())


def normalize(job: dict) -> dict:
    """Приводит одну вакансию Remotive к нашему единому виду."""
    # job_type — строка ("full_time"), а в нашей схеме job_types список.
    # Оборачиваем в список из одного элемента; если поля нет или оно
    # пустое — пустой список, а не [None].
    job_type = job.get("job_type")
    job_types = [job_type] if job_type else []

    return {
        "source": "remotive",
        # id приходит числом. Приводим к строке на входе — по той же
        # причине, что и у remoteok: иначе автодетект схемы в BigQuery
        # сделает колонку int64 там, где у других источников string,
        # и union all в staging развалится на несовпадении типов.
        "source_id": str(job["id"]),
        "title": job.get("title"),
        "company_name": job.get("company_name"),
        # Где готов работать кандидат: "Europe", "USA", "Worldwide"
        # или перечисление через запятую.
        "location": job.get("candidate_required_location"),
        "remote": True,                       # сайт только про удалёнку
        "url": job.get("url"),
        "tags": job.get("tags") or [],
        "job_types": job_types,
        "description": job.get("description"),
        "created_at_unix": to_unix(job.get("publication_date")),
        # Зарплата свободным текстом, как её написал работодатель.
        # Разбор («$20k -$35k» → 20000 и 35000) — это уже бизнес-логика,
        # ей место в dbt, а не в сыром слое.
        "salary_text": job.get("salary") or None,
        "salary_min": None,                   # источник чисел не отдаёт
        "salary_max": None,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }


def collect() -> list[dict]:
    """Забирает вакансии и приводит их к нашему виду."""
    jobs = fetch_all()

    result = []
    for job in jobs:
        result.append(normalize(job))

    return result


def save(rows: list[dict], path: Path = OUTPUT_PATH) -> None:
    """Дописывает строки в файл. Каждая вакансия — отдельная строка JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # "a" значит append — дописать в конец, не стирая то, что было.
    # Дубли нас сейчас не пугают: сырой слой имеет право их содержать,
    # убирать повторы будем в dbt.
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"записано {len(rows)} строк в {path}")


if __name__ == "__main__":
    rows = collect()
    save(rows)
