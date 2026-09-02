"""
ШАГ 1. Первый коллектор: забираем вакансии с сайта arbeitnow.com.

Что делает этот файл, по-человечески:
  1. Ходит по адресу https://www.arbeitnow.com/api/job-board-api и просит
     отдать список вакансий. Никакой регистрации и ключей не нужно.
  2. Получает ответ в формате JSON — это обычный текст, который выглядит
     как питоновский словарь: {"data": [ {вакансия}, {вакансия}, ... ]}.
  3. Из каждой вакансии оставляет только нужные нам поля и приводит их
     к одинаковому виду (это называется "нормализация").
  4. Складывает результат в файл data/raw_arbeitnow.jsonl —
     по одной вакансии в строке.

Как запустить (в терминале, из папки проекта):
    pip install requests
    python -m collectors.arbeitnow

Почему первый коллектор мы пишем руками, а не берём готовую библиотеку:
чтобы вы своими глазами увидели, что "загрузка данных из источника" —
это тридцать строк кода, а не магия. Остальные три источника потом
подключим через библиотеку dlt, и вы будете понимать, что она делает за вас.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# Адрес источника. Открытый, ключ не нужен.
API_URL = "https://www.arbeitnow.com/api/job-board-api"

# Куда складываем сырой результат.
# .jsonl — это "json lines": один JSON-объект в каждой строке файла.
# Такой формат удобен тем, что файл можно дописывать, не перечитывая целиком.
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_arbeitnow.jsonl"

# Сколько страниц забираем за один запуск. Начните с 3, чтобы быстро увидеть результат.
MAX_PAGES = 3

# Пауза между запросами, в секундах.
# Правило вежливости: не долбить чужой сервер в полную силу.
SLEEP_BETWEEN_PAGES = 1.0


def fetch_page(page: int) -> list[dict]:
    """Забирает одну страницу вакансий и возвращает список словарей.

    requests.get(...) — это ровно то же самое, что открыть ссылку в браузере,
    только результат приходит в программу, а не на экран.
    """
    response = requests.get(
        API_URL,
        params={"page": page},
        timeout=30,
        headers={"User-Agent": "vacancy-radar/0.1 (learning project)"},
    )
    # Если сервер ответил ошибкой (404, 500 и т.п.) — упасть сразу и громко,
    # а не тащить дальше пустоту. Это хорошая привычка: ошибки должны быть видны.
    response.raise_for_status()

    payload = response.json()
    # У этого API все вакансии лежат под ключом "data".
    return payload.get("data", [])


def normalize(job: dict, page: int) -> dict:
    """Приводит одну вакансию к нашему единому виду.

    Здесь мы НЕ чистим и НЕ обогащаем данные — только раскладываем по полям
    и добавляем служебную информацию (откуда и когда взяли).
    Вся настоящая обработка будет позже, в dbt. Это важный принцип:
    сырой слой хранит данные как есть, чтобы всегда можно было пересчитать заново.
    """
    return {
        # --- поля источника ---
        "source": "arbeitnow",
        "source_id": job.get("slug"),
        "title": job.get("title"),
        "company_name": job.get("company_name"),
        "location": job.get("location"),
        "remote": job.get("remote"),
        "url": job.get("url"),
        "tags": job.get("tags") or [],
        "job_types": job.get("job_types") or [],
        "description": job.get("description"),
        # created_at приходит как unix-время (число секунд с 1970 года).
        # Оставляем и число, и человекочитаемую дату — пригодится обоим.
        "created_at_unix": job.get("created_at"),
        # --- служебные поля, которых в источнике не было ---
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source_page": page,
    }


def collect(max_pages: int = MAX_PAGES) -> list[dict]:
    """Обходит страницы и возвращает список нормализованных вакансий."""
    collected: list[dict] = []

    for page in range(1, max_pages + 1):
        jobs = fetch_page(page)

        # Пустая страница означает, что вакансии закончились — дальше идти незачем.
        if not jobs:
            print(f"страница {page}: пусто, останавливаюсь")
            break

        for job in jobs:
            collected.append(normalize(job, page))

        print(f"страница {page}: получено {len(jobs)} вакансий")
        time.sleep(SLEEP_BETWEEN_PAGES)

    return collected


def save(rows: list[dict], path: Path = OUTPUT_PATH) -> None:
    """Дописывает строки в файл. Каждая вакансия — отдельная строка JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)

    # "a" значит append — дописать в конец, не стирая то, что было.
    # Дубли нас сейчас не пугают: сырой слой имеет право их содержать,
    # убирать повторы будем в dbt. Это осознанное решение, а не небрежность.
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"записано {len(rows)} строк в {path}")


if __name__ == "__main__":
    rows = collect()
    save(rows)
