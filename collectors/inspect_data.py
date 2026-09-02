"""
ШАГ 1.5. Посмотреть глазами на то, что собралось.

Пока данных мало, самая полезная привычка — открыть их и посмотреть,
а не сразу строить поверх них графики. Этот скрипт печатает:
  - сколько всего строк,
  - сколько из них уникальных,
  - примеры заголовков,
  - какие поля часто оказываются пустыми.

Запуск:
    python -m collectors.inspect_data
    python -m collectors.inspect_data data/raw_remoteok.jsonl

Без аргумента смотрит файл arbeitnow — так же, как раньше.
С аргументом смотрит тот файл, который назвали.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "raw_arbeitnow.jsonl"


def resolve_path() -> Path:
    """Решает, какой файл смотреть: названный в командной строке или файл по умолчанию.

    sys.argv — список слов, которыми запустили скрипт.
    Для `python -m collectors.inspect_data data/raw_remoteok.jsonl` он равен
    ['.../inspect_data.py', 'data/raw_remoteok.jsonl'].
    Нулевой элемент — сам скрипт, наш первый аргумент лежит под индексом 1.
    """
    if len(sys.argv) > 1:
        return Path(sys.argv[1])
    return DATA_PATH


def load_rows(path: Path = DATA_PATH) -> list[dict]:
    if not path.exists():
        raise SystemExit(
            f"Файла {path} нет. Сначала запустите нужный коллектор"
        )
    with path.open(encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def main() -> None:
    path = resolve_path()
    rows = load_rows(path)

    print(f"файл:                   {path}")
    print(f"всего строк:            {len(rows)}")
    print(f"уникальных source_id:   {len({r['source_id'] for r in rows})}")
    print(f"уникальных компаний:    {len({r['company_name'] for r in rows})}")
    print(f"помечено как remote:    {sum(1 for r in rows if r.get('remote'))}")

    print("\nпримеры вакансий:")
    for row in rows[:5]:
        print(f"  · {row['title']} — {row['company_name']} ({row['location']})")

    print("\nпустых значений по полям:")
    for field in ("title", "company_name", "location", "description", "created_at_unix"):
        empty = sum(1 for r in rows if not r.get(field))
        print(f"  {field:<18} {empty}")

    print("\nсамые частые города:")
    for city, count in Counter(r.get("location") for r in rows).most_common(8):
        print(f"  {count:>3}  {city}")


if __name__ == "__main__":
    main()
