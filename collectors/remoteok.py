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
from datetime import datetime, timezone
from pathlib import Path

import requests

# Адрес источника. Открытый, ключ не нужен.
API_URL = "https://remoteok.com/api"

# Куда складываем сырой результат.
# .jsonl — это "json lines": один JSON-объект в каждой строке файла.
# Такой формат удобен тем, что файл можно дописывать, не перечитывая целиком.
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_remoteok.jsonl"

def fetch_all() -> list[dict]:
    """Забирает весь список вакансий одним запросом."""
    response = requests.get(API_URL, timeout=30,
                            headers={"User-Agent": "vacancy-radar/0.1 (learning project)"})
    response.raise_for_status()
    items = response.json()

    jobs = []
    for item in items:
        if item.get("id"):
            jobs.append(item)

    print(f"получено элементов: {len(items)}, из них вакансий: {len(jobs)}")
    return jobs


def normalize(job: dict) -> dict:
    """Приводит одну вакансию RemoteOK к нашему единому виду."""
    return {
        "source": "remoteok",
        # RemoteOK отдаёт id то числом, то строкой в кавычках. Если пустить
        # это как есть, автодетект схемы в BigQuery выберет тип по первой
        # загрузке — и в raw окажется int64 там, где у других источников
        # строка. Фиксируем тип здесь, на входе. Ключ берём по [], а не
        # .get(): вакансии без id отсеяны в fetch_all, и если такая всё же
        # дойдёт сюда — пусть скрипт упадёт, а не запишет "None".
        "source_id": str(job["id"]),
        "title": job.get("position"),          # какое поле у RemoteOK?
        "company_name": job.get("company"),   # и здесь
        "location": job.get("location"),
        "remote": True,                      # сайт только про удалёнку
        "url": job.get("url"),
        "tags": job.get("tags") or [],
        "job_types": [],                     # такого поля у источника нет
        "description": job.get("description"),
        "created_at_unix": job.get("epoch"),  # время публикации
        "salary_min": job.get("salary_min") or None,
        "salary_max": job.get("salary_max") or None,
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
    # убирать повторы будем в dbt. Это осознанное решение, а не небрежность.
    with path.open("a", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"записано {len(rows)} строк в {path}")


if __name__ == "__main__":
    rows = collect()
    save(rows)
