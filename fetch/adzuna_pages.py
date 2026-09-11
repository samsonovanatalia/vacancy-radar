"""
Догрузка полного текста вакансий Adzuna со страниц сайта.

Зачем: API Adzuna отдаёт описание, обрезанное на 500 символах. Полный текст
есть на странице вакансии — в разметке JSON-LD с типом JobPosting, которую
сайт держит для Google Jobs.

Что делает скрипт:
  1. Берёт вакансии Adzuna из mart_vacancies_for_me, по которым в
     raw.vacancy_pages нет свежего окончательного ответа. Не больше MAX_PER_RUN.
  2. Для каждой скачивает страницу по сохранённой ссылке из API — целиком,
     с параметрами, — с честным User-Agent и паузой между запросами.
     Ссылки /land/ad/ не запрашивает: robots.txt этот путь запрещает.
  3. Пишет в raw.vacancy_pages строку на каждую вакансию: код ответа, конечный
     адрес, есть ли разметка JobPosting и — только у живой страницы — текст.

Почему ссылку берём целиком, с параметрами: на голый /details/<id> CloudFront
перед сайтом отвечает 403 любому клиенту не из браузера, а по ссылке из API
(?utm_medium=api&utm_source=<app_id>) страница открывается — проверено
2026-09-11. robots.txt такие ссылки разрешает, а путь /land/ad/ — запрещает.

Окончательный ответ — код 200 с текстом или код 404. 403, таймауты, 5xx и
сетевые ошибки окончательными не считаются: вакансия остаётся кандидатом, и
следующий прогон запросит её снова. Окончательный ответ тоже устаревает —
через RECHECK_AFTER_DAYS дней: вакансию могут снять в любой момент.

Почему текст пишем как есть, с HTML-разметкой: raw хранит то, что пришло из
источника. Чистит текст dbt — так же, как description в stg_vacancies.

Жива ли вакансия, скрипт не решает: он записывает код ответа и признак
разметки, а что из них следует — решается в dbt.

Перед запуском:
    export BQ_PROJECT=ваш-project-id
    export GOOGLE_APPLICATION_CREDENTIALS=/путь/к/ключу.json

Как запустить (из папки проекта):
    python -m fetch.adzuna_pages        # до MAX_PER_RUN вакансий
    python -m fetch.adzuna_pages 3      # пробный прогон на трёх
"""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

import httpx
from bs4 import BeautifulSoup
from google.cloud import bigquery

MARTS_DATASET = "dbt_natalia_marts"
RAW_DATASET = "raw"
TABLE_NAME = "vacancy_pages"

# Честная подпись: кто спрашивает и зачем. Браузер не изображаем.
USER_AGENT = "vacancy-radar/1.0 (personal job search)"

# Таймаут одного запроса, секунды: и на соединение, и на ожидание ответа.
REQUEST_TIMEOUT_SECONDS = 30

# Пауза после каждого запроса, секунды. Для нас robots.txt паузу не задаёт
# (Crawl-delay есть только у bingbot — 2 секунды); берём 3 с запасом: это
# чужой сайт, а не API, который рассчитан на частые обращения.
PAUSE_SECONDS = 3

# Временные сбои — через паузу запрос обычно проходит: 429 — просят сбавить
# темп; 500, 502, 503, 504 — сервер споткнулся или перегружен. Плюс таймаут
# и ошибка соединения (см. fetch_page). Остальные коды — 403, 404 и прочие —
# не повторяем: это ответ сайта, он и запишется в таблицу.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# Паузы перед повторами, секунды. Сколько чисел — столько повторов, как в
# collectors/adzuna.py: первая попытка и до трёх повторов.
RETRY_PAUSES = [5, 15, 45]

# Пути, которые robots.txt Adzuna запрещает всем роботам (User-agent: *) и
# которые встречаются в ссылках из API. По таким ссылкам не ходим, а пишем
# строку с причиной. Прочие запреты robots.txt касаются поиска и параметров,
# которых в ссылках на вакансии нет.
DISALLOWED_PATH_PREFIXES = ("/land/ad/", "/jobs/land/ad/")
SKIP_REASON = "не запрашивали: путь /land/ad/ запрещён в robots.txt"

# Потолок на прогон. 60 — с запасом на всю подборку Adzuna (в ней несколько
# десятков вакансий): при паузе PAUSE_SECONDS это около четырёх минут.
# Аргументом командной строки потолок можно опустить, но не поднять — как в
# enrich/with_gemini.py.
MAX_PER_RUN = 60

# Через сколько дней окончательный ответ устаревает и вакансию проверяем
# снова. Живость — не навсегда: вакансию, однажды ответившую 200, могут
# снять на следующий день, а без повторной проверки она так и числилась бы
# живой.
RECHECK_AFTER_DAYS = 2

# Сколько строк копить перед записью в BigQuery. Порциями, а не одним куском
# в конце: если процесс убьют извне, пропадёт только последняя неполная порция.
BATCH_SIZE = 10

# Схема явная, а не autodetect: page_text, final_url и http_status бывают
# null во всех строках порции, и autodetect не создал бы такие колонки.
TABLE_SCHEMA = [
    bigquery.SchemaField("vacancy_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("source_id", "STRING", mode="REQUIRED"),
    # Сохранённая ссылка из API — по ней и запрашиваем. У пропущенной
    # вакансии — ссылка, по которой не пошли.
    bigquery.SchemaField("url_requested", "STRING", mode="REQUIRED"),
    # null — ответа нет: сетевая ошибка или пропуск, причина в fetch_error.
    bigquery.SchemaField("http_status", "INT64"),
    # Адрес страницы, на которой закончили после редиректов.
    bigquery.SchemaField("final_url", "STRING"),
    # Есть ли на странице JSON-LD с типом JobPosting — при любом коде ответа.
    # null — только если страницы нет (сетевая ошибка, пропуск) или разметка
    # не разобралась: false значило бы «разметки нет», а этого мы не знаем.
    bigquery.SchemaField("has_job_posting_ld", "BOOL"),
    # Поле description из JobPosting как есть, с HTML. Только при коде 200 и
    # найденной разметке, иначе null.
    bigquery.SchemaField("page_text", "STRING"),
    # Длина page_text в символах; null, когда page_text null.
    bigquery.SchemaField("text_length", "INT64"),
    bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
    # Почему попытка не дала результата: сетевая ошибка, временный код после
    # всех повторов, неразборчивый JSON-LD или пропуск по robots.txt.
    # null — попытка прошла штатно, каким бы ни был код ответа.
    bigquery.SchemaField("fetch_error", "STRING"),
]


def read_args() -> argparse.Namespace:
    """Разбирает командную строку: необязательное число вакансий."""
    parser = argparse.ArgumentParser(description="Догрузка полного текста вакансий Adzuna.")
    # nargs="?" — аргумент необязателен; нет его — берётся default.
    parser.add_argument(
        "limit", nargs="?", type=int, default=MAX_PER_RUN,
        help=f"сколько вакансий обработать, не больше {MAX_PER_RUN}",
    )
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("число вакансий должно быть больше нуля")
    args.limit = min(args.limit, MAX_PER_RUN)
    return args


def ensure_table(bq: bigquery.Client, table_id: str) -> None:
    """Создаёт raw.vacancy_pages с явной схемой, если таблицы ещё нет.

    Заранее, а не при первой записи: запрос кандидатов обращается к этой
    таблице, и без неё упал бы уже первый прогон.
    """
    table = bigquery.Table(table_id, schema=TABLE_SCHEMA)
    # Партиция по дню скачивания — как raw-таблицы вакансий по ingested_at.
    table.time_partitioning = bigquery.TimePartitioning(field="fetched_at")
    bq.create_table(table, exists_ok=True)


def fetch_candidates(bq: bigquery.Client, project: str, limit: int) -> list[dict]:
    """Вакансии Adzuna из подборки, по которым ещё нет окончательного ответа."""
    # Первое not exists: свежий окончательный ответ — код 200 с текстом или
    # код 404, полученный за последние RECHECK_AFTER_DAYS дней. Ответ старше
    # вакансию не исключает: живость устарела, и её пора проверить снова.
    # Строки с 403, таймаутами, 5xx и сетевыми ошибками вакансию не
    # исключают вовсе — она остаётся кандидатом на следующий прогон.
    #
    # Второе условие: ссылку /land/ad/ мы не запрашиваем, а пишем строку-
    # пропуск. Если пропуск уже записан и ссылка всё ещё /land/ad/, писать
    # ту же строку каждый прогон незачем — да и место в лимите она заняла
    # бы зря. Строку-пропуск узнаём по url_requested с /land/ad/: по таким
    # адресам скрипт никогда не ходит. Сменится ссылка на /details/ —
    # условие перестанет выполняться, и вакансия снова станет кандидатом.
    #
    # not exists, а не not in (select ...): окажись в подзапросе хоть один
    # null, not in не вернул бы ничего.
    #
    # source_id берём из mart_vacancies_scored: в mart_vacancies_for_me этой
    # колонки нет, а выковыривать её из vacancy_key — значит полагаться на
    # формат ключа.
    #
    # Порядок: сначала вакансии с меньшим числом прошлых попыток — ни разу не
    # пробованные первыми, — потом по релевантности. Иначе вакансии, которые
    # сайт стабильно не отдаёт (403, таймауты), каждый прогон вставали бы в
    # начало очереди и при лимите MAX_PER_RUN не пускали бы к остальным.
    # Попытка — любая строка по вакансии в raw.vacancy_pages.
    #
    # Попытки считаем заранее в attempts и присоединяем left join: у ни разу
    # не пробованной вакансии строки в attempts нет, и coalesce превращает
    # её null в 0.
    #
    # f-строка здесь безопасна: имена и RECHECK_AFTER_DAYS — наши константы,
    # limit — уже int.
    query = f"""
        with attempts as (
            select
                vacancy_key,
                count(*) as attempts_count
            from `{project}.{RAW_DATASET}.{TABLE_NAME}`
            group by vacancy_key
        )

        select
            f.vacancy_key,
            s.source_id,
            f.url
        from `{project}.{MARTS_DATASET}.mart_vacancies_for_me` as f
        join `{project}.{MARTS_DATASET}.mart_vacancies_scored` as s
          on s.vacancy_key = f.vacancy_key
        left join attempts as a
          on a.vacancy_key = f.vacancy_key
        where f.source = 'adzuna'
          and not exists (
              select 1
              from `{project}.{RAW_DATASET}.{TABLE_NAME}` as p
              where p.vacancy_key = f.vacancy_key
                and ((p.http_status = 200 and p.page_text is not null)
                     or p.http_status = 404)
                and p.fetched_at >= timestamp_sub(current_timestamp(), interval {RECHECK_AFTER_DAYS} day)
          )
          and not (
              strpos(f.url, '/land/ad/') > 0
              and exists (
                  select 1
                  from `{project}.{RAW_DATASET}.{TABLE_NAME}` as p
                  where p.vacancy_key = f.vacancy_key
                    and strpos(p.url_requested, '/land/ad/') > 0
              )
          )
        order by coalesce(a.attempts_count, 0), f.relevance_score desc, f.posted_at desc
        limit {limit}
    """
    return [dict(row) for row in bq.query(query).result()]


def find_job_posting(html: str) -> tuple[bool | None, str | None, str | None]:
    """Ищет в странице разметку JSON-LD с типом JobPosting.

    Возвращает три значения: есть ли разметка; поле description из неё как
    есть; описание ошибки разбора. Если JobPosting не нашёлся, а какой-то
    блок JSON-LD не разобрался, наличие разметки неизвестно — None и ошибка.
    """
    soup = BeautifulSoup(html, "html.parser")
    parse_error = None
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            # tag.string — текст внутри <script>; у пустого тега это None.
            data = json.loads(tag.string or "")
        except json.JSONDecodeError as error:
            parse_error = f"JSON-LD не разбирается: {error}"
            continue
        # Разметка бывает объектом, списком объектов или объектом с @graph.
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("@graph", [data])
        else:
            items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            # @type — строка "JobPosting" или список типов.
            item_type = item.get("@type")
            if item_type == "JobPosting" or (isinstance(item_type, list) and "JobPosting" in item_type):
                return True, item.get("description"), None
    if parse_error is not None:
        return None, None, parse_error
    return False, None, None


def fetch_page(client: httpx.Client, url: str) -> tuple[httpx.Response | None, str | None]:
    """GET страницы; при временном сбое ждёт и повторяет.

    Возвращает ответ и описание сбоя. Ответ есть при любом HTTP-коде — даже
    если повторы не помогли, тогда рядом и сбой. Ответа нет только при
    сетевой ошибке.
    """
    attempts = len(RETRY_PAUSES) + 1
    for attempt in range(1, attempts + 1):
        response = None
        try:
            response = client.get(url)
        except (httpx.TimeoutException, httpx.ConnectError) as error:
            # Ответ не пришёл за REQUEST_TIMEOUT_SECONDS или не удалось
            # соединиться — это обычно проходит, повторяем.
            problem = f"сетевая ошибка {type(error).__name__}: {error}"
        except httpx.RequestError as error:
            # Прочие ошибки запроса (оборванное соединение, слишком много
            # редиректов) не повторяем.
            return None, f"сетевая ошибка {type(error).__name__}: {error}"
        else:
            if response.status_code not in RETRY_STATUS_CODES:
                return response, None
            problem = f"код {response.status_code}"

        if attempt == attempts:
            return response, f"{problem}, не помогли {attempts} попытки"
        # attempt начинается с 1, а список пауз — с 0.
        pause = RETRY_PAUSES[attempt - 1]
        print(f"  {url}: {problem}, попытка {attempt} из {attempts}, повтор через {pause} с", flush=True)
        time.sleep(pause)
    # Сюда не дойдём: последняя попытка всегда что-то возвращает.
    raise AssertionError("недостижимо")


def to_row(
    vacancy: dict,
    response: httpx.Response | None,
    has_ld: bool | None,
    page_text: str | None,
    problem: str | None,
) -> dict:
    """Строка для raw.vacancy_pages. response=None — ответа нет: сбой сети или пропуск."""
    return {
        "vacancy_key": vacancy["vacancy_key"],
        "source_id": vacancy["source_id"],
        "url_requested": vacancy["url"],
        "http_status": response.status_code if response is not None else None,
        "final_url": str(response.url) if response is not None else None,
        "has_job_posting_ld": has_ld,
        "page_text": page_text,
        "text_length": len(page_text) if page_text is not None else None,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
        "fetch_error": problem,
    }


def save(bq: bigquery.Client, table_id: str, rows: list[dict]) -> None:
    """Дописывает строки в raw.vacancy_pages загрузкой, а не потоковой вставкой."""
    # load job, как в enrich/with_gemini.py: бесплатен, строки сразу видны
    # запросам. Потоковая вставка (insert_rows_json) платная.
    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    bq.load_table_from_json(rows, table_id, job_config=job_config).result()
    print(f"записано {len(rows)} строк в {RAW_DATASET}.{TABLE_NAME}", flush=True)


def fetch_pages(limit: int) -> None:
    """Скачивает страницы до limit кандидатов, пишет строки и печатает итог."""
    project = os.environ["BQ_PROJECT"]        # упадёт с понятной ошибкой, если не задан
    bq = bigquery.Client(project=project)
    table_id = f"{project}.{RAW_DATASET}.{TABLE_NAME}"
    ensure_table(bq, table_id)

    candidates = fetch_candidates(bq, project, limit)
    print(f"кандидатов: {len(candidates)}", flush=True)

    rows: list[dict] = []             # строки, ещё не записанные в BigQuery
    written = 0                       # сколько строк уже записано
    requested = 0                     # сколько страниц запрошено (пропуски не в счёт)
    downloaded = 0                    # сколько ответов 200 или 404
    blocked = 0                       # сколько ответов 403
    with_text = 0                     # у скольких страниц сохранён текст
    statuses: dict[str, int] = {}     # код ответа → сколько раз
    skipped: dict[str, str] = {}      # vacancy_key → причина пропуска
    failed: dict[str, str] = {}       # vacancy_key → причина сбоя

    # Сбой на одной странице не прерывает прогон: строка с fetch_error
    # пишется, как любая другая, и скрипт идёт к следующей вакансии.
    #
    # follow_redirects: страница может прийти через редирект, и нам нужны
    # код и адрес конечной страницы. Без него httpx вернул бы сам ответ 30x.
    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        timeout=REQUEST_TIMEOUT_SECONDS,
        follow_redirects=True,
    ) as client:
        try:
            for number, vacancy in enumerate(candidates, start=1):
                key = vacancy["vacancy_key"]
                url = vacancy["url"]
                progress = f"{number}/{len(candidates)} {key}"

                if urlsplit(url).path.startswith(DISALLOWED_PATH_PREFIXES):
                    # Запроса нет — и паузы после него не нужно.
                    skipped[key] = SKIP_REASON
                    rows.append(to_row(vacancy, None, None, None, SKIP_REASON))
                    print(f"{progress} пропущена: {SKIP_REASON}", flush=True)
                else:
                    response, problem = fetch_page(client, url)
                    requested += 1

                    if response is None:
                        has_ld, description = None, None
                    else:
                        has_ld, description, parse_error = find_job_posting(response.text)
                        problem = problem or parse_error

                    # Текст — только с живой страницы: код 200 и разметка
                    # JobPosting. Страница снятой вакансии отдаёт 404, но
                    # описание на ней остаётся. Сохрани его — и мёртвая
                    # вакансия станет неотличима от живой.
                    page_text = None
                    if response is not None and response.status_code == 200 and has_ld:
                        page_text = description
                    rows.append(to_row(vacancy, response, has_ld, page_text, problem))

                    status = str(response.status_code) if response is not None else "нет ответа"
                    statuses[status] = statuses.get(status, 0) + 1
                    if status in ("200", "404"):
                        downloaded += 1
                    if status == "403":
                        blocked += 1
                    if page_text is not None:
                        with_text += 1
                    line = (f"{progress} → код {status}, JobPosting: {has_ld}, "
                            f"текст: {len(page_text) if page_text is not None else '—'}")
                    if problem is not None:
                        failed[key] = problem
                        line += f", сбой: {problem}"
                    print(line, flush=True)

                    # Пауза после каждого запроса, включая последний: условие
                    # «кроме последнего» — лишняя ветка ради трёх секунд.
                    time.sleep(PAUSE_SECONDS)

                if len(rows) == BATCH_SIZE:
                    save(bq, table_id, rows)
                    written += len(rows)
                    rows = []
        finally:
            # finally дописывает неполную порцию и печатает итог даже при падении.
            if rows:
                save(bq, table_id, rows)
                written += len(rows)
            codes = ", ".join(f"{code} — {count}" for code, count in sorted(statuses.items()))
            print(
                f"итог: вакансий {len(candidates)}: запрошено {requested}, "
                f"скачано {downloaded}, с текстом {with_text}, пропущено {len(skipped)}, "
                f"сбоев {len(failed)}; коды ответа: {codes or 'нет'}; записано строк {written}",
                flush=True,
            )
            for skipped_key, reason in skipped.items():
                print(f"  пропуск: {skipped_key} — {reason}", flush=True)
            for failed_key, reason in failed.items():
                print(f"  сбой: {failed_key} — {reason}", flush=True)

    # Код 1 — авария: страницы запрашивали, а не скачалась ни одна (нет ни
    # одного ответа 200 или 404). Главный случай — все запросы получили 403:
    # сайт блокирует скрипт. Хоть одна страница скачалась — код 0. Пропуски
    # /land/ad/ запросами не считаются: если были только они, код 0.
    if requested and not downloaded:
        if blocked == requested:
            raise SystemExit(
                f"Авария: все {requested} запросов получили 403 — сайт блокирует запросы скрипта."
            )
        raise SystemExit(
            f"Не скачалась ни одна страница из {requested} запрошенных: причины — в итоге выше."
        )


if __name__ == "__main__":
    args = read_args()
    fetch_pages(args.limit)
