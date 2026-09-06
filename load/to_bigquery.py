"""
ШАГ 2 (вторая неделя). Загрузка собранного файла в BigQuery.

Сейчас этот файл — заготовка с подробными комментариями. Запускать его
можно будет после того, как заведёте проект в Google Cloud и скачаете ключ.
Читать его полезно уже сейчас: видно, что «загрузка в облако» — это
двадцать строк, а не отдельная профессия.

Скрипт не привязан к одному источнику: имя источника передаётся первым
аргументом командной строки, по умолчанию arbeitnow.

Что произойдёт при запуске:
  1. Библиотека прочитает файл data/raw_<источник>.jsonl.
  2. Создаст (если ещё нет) датасет raw и таблицу <источник>.
  3. Допишет строки в таблицу, не стирая старые.

Перед запуском:
    pip install google-cloud-bigquery
    export GOOGLE_APPLICATION_CREDENTIALS=/путь/к/ключу.json
    export BQ_PROJECT=ваш-project-id

Как запустить:
    python -m load.to_bigquery              # arbeitnow, значение по умолчанию
    python -m load.to_bigquery remoteok     # любой другой источник
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Импорт намеренно внутри функции: пока пакет не установлен,
# файл всё равно можно открыть и прочитать, не получив ошибку.

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATASET = "raw"
DEFAULT_SOURCE = "arbeitnow"


def load(source: str = DEFAULT_SOURCE) -> None:
    """Заливает data/raw_<source>.jsonl в таблицу raw.<source>."""
    from google.cloud import bigquery

    # Всё, что зависит от источника, выводится из его имени: путь к файлу
    # и имя таблицы. Остальная логика загрузки одна на все источники.
    data_path = DATA_DIR / f"raw_{source}.jsonl"

    project = os.environ["BQ_PROJECT"]        # упадёт с понятной ошибкой, если не задан
    client = bigquery.Client(project=project)
    table_id = f"{project}.{DATASET}.{source}"

    # Датасет — это папка для таблиц. Создаём, если её ещё нет.
    dataset_ref = bigquery.Dataset(f"{project}.{DATASET}")
    dataset_ref.location = "EU"               # данные храним в Европе
    client.create_dataset(dataset_ref, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        # Наш файл — json lines, ровно этот формат BigQuery понимает нативно.
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        # Схему (список колонок и типов) BigQuery определит сам по данным.
        autodetect=True,
        # WRITE_APPEND = дописать в конец. Сырой слой только растёт.
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        # Разбиваем таблицу по дню загрузки: запрос за один день будет читать
        # один кусок, а не всю историю. Это и есть партиционирование,
        # то самое, из-за которого запросы стоят дёшево.
        time_partitioning=bigquery.TimePartitioning(field="ingested_at"),
    )

    with data_path.open("rb") as f:
        job = client.load_table_from_file(f, table_id, job_config=job_config)

    job.result()   # ждём окончания загрузки; если упало — увидим ошибку здесь

    table = client.get_table(table_id)
    print(f"в таблице {DATASET}.{source} теперь {table.num_rows} строк")


if __name__ == "__main__":
    # sys.argv[0] — имя самого скрипта, поэтому источник берём из sys.argv[1].
    source_name = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SOURCE
    load(source_name)
