"""
Отправка вакансий из очереди в Telegram.

Что делает скрипт:
  1. Читает очередь mart_digest_queue — все подходящие вакансии, у которых
     ещё нет исхода отправки, — без тех, что уже записаны в raw.digest_sent
     (см. fetch_queue).
  2. Берёт первые MAX_PER_RUN по queue_position. Отправляет шапку «Дайджест
     за <дата>, вакансий: N» — или «25 из 64, остальные завтра», если очередь
     длиннее, — затем по одному сообщению на вакансию. Очередь пуста — одно
     сообщение «сегодня новых вакансий нет»: молчание бота не должно
     выглядеть так же, как поломка.
  3. После каждой вакансии с окончательным исходом дописывает строку в
     raw.digest_sent: vacancy_key, время и status:
       sent          — Telegram подтвердил доставку;
       undeliverable — Telegram отказал с кодом 400: такое сообщение не уйдёт
                       и завтра, а без записи вакансия вечно занимала бы
                       очередь.
     Временный сбой (429, 5xx, сеть) не пишется: вакансия остаётся в очереди
     и уйдёт в следующий раз. Следующая сборка dbt уберёт из очереди всё
     записанное (stg_digest_sent → mart_digest_queue).

Почему лимит, а не вся очередь сразу: очередь копит вакансии за 30 дней, и
сотня сообщений подряд — уже не подборка, а лента. Лимит откладывает, а не
выбрасывает: вакансия, не вошедшая в первые MAX_PER_RUN, ничего не
записывает и остаётся в очереди. В этом отличие от прежнего топ-10, где
одиннадцатая вакансия не приходила никогда.

Почему по сообщению на вакансию, а не одним куском: каждую вакансию можно
переслать отдельно, и про каждую точно известно, дошла ли она. Одно большое
сообщение дошло бы целиком или не дошло вовсе, да ещё упёрлось бы в лимит
Telegram — 4096 символов на сообщение.

Таблицу raw.digest_sent скрипт не создаёт: её создаёт и дополняет колонкой
status sql/raw_digest_sent.sql. Нет таблицы — скрипт упадёт на первом же
запросе.

Перед запуском:
    export BQ_PROJECT=ваш-project-id
    export GOOGLE_APPLICATION_CREDENTIALS=/путь/к/ключу.json
    export TELEGRAM_BOT_TOKEN=токен-от-BotFather
    export TELEGRAM_CHAT_ID=id-чата

Как запустить (из папки проекта):
    python -m notify.telegram
"""

from __future__ import annotations

import html
import os
import re
import time
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import httpx
from google.cloud import bigquery

MARTS_DATASET = "dbt_natalia_marts"
RAW_DATASET = "raw"
TABLE_NAME = "digest_sent"

# Сколько вакансий отправлять за один запуск. Остальные остаются в очереди и
# уйдут в следующий раз — в порядке queue_position. 25 сообщений ещё можно
# прочитать за раз, и прогон с паузами и записью в BigQuery укладывается в
# пару минут.
MAX_PER_RUN = 25

# Дата в шапке — по Барселоне, а не по часам машины, на которой идёт скрипт:
# на раннере GitHub часы в UTC, а дата должна совпадать с календарём читателя.
TIMEZONE = ZoneInfo("Europe/Madrid")

# Таймаут одного запроса к Telegram, секунды.
REQUEST_TIMEOUT_SECONDS = 30

# Пауза после каждого сообщения, секунды. Telegram просит не отправлять в
# один чат больше одного сообщения в секунду; чаще — ответит 429.
PAUSE_SECONDS = 1

# Временные сбои — через паузу запрос обычно проходит: 429 — Telegram просит
# сбавить темп; 500, 502, 503, 504 — сервер споткнулся. Плюс таймаут и ошибка
# соединения (см. send_message). Остальные коды от повтора не починятся.
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}

# Паузы перед повторами, секунды, как в fetch/adzuna_pages.py: первая попытка
# и до трёх повторов. На 429 ждём дольше из двух чисел — этой паузы или
# retry_after, который Telegram присылает в ответе.
RETRY_PAUSES = [5, 15, 45]

# Код ответа, после которого вакансия записывается как undeliverable.
# Только 400, «Bad Request»: Telegram не принимает само сообщение (разметка
# не разбирается, текст длиннее 4096 символов), и завтра ответ будет тем же.
# 401 (неверный токен) и 403 (бот заблокирован) тоже не лечатся повтором, но
# сломано там не сообщение, а доступ: пометь мы вакансии недоставляемыми,
# после починки токена они бы уже не вернулись.
UNDELIVERABLE_STATUS_CODE = 400

# Сколько предложений из summary печатать. Модель пишет 2–3, в сообщении
# хватает двух.
SUMMARY_SENTENCES = 2

# Подписи к значениям из ответа модели. unclear в словарях нет: «модель не
# поняла» — то же, что пустое поле, и такую строку не печатаем.
WORK_MODE_LABELS = {"remote": "удалённо", "hybrid": "гибрид", "onsite": "офис"}
SALARY_PERIOD_LABELS = {"year": "в год", "month": "в месяц", "day": "в день", "hour": "в час"}

# Схема явная, как в остальных модулях, и та же, что в sql/raw_digest_sent.sql.
TABLE_SCHEMA = [
    bigquery.SchemaField("vacancy_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sent_at", "TIMESTAMP", mode="REQUIRED"),
    # Необязательная, хотя бот пишет status всегда: в таблице колонка
    # необязательная (см. sql/raw_digest_sent.sql), а схема загрузки должна
    # совпадать со схемой таблицы.
    bigquery.SchemaField("status", "STRING"),
]


def read_setting(name: str) -> str:
    """Достаёт настройку из окружения; без неё падаем сразу и внятно."""
    # get и проверка на пустоту, а не os.environ[name]: в GitHub Actions
    # незаданный секрет приходит пустой строкой, и KeyError бы не случилось.
    value = os.environ.get(name)
    if not value:
        raise SystemExit(f"Не задана переменная окружения {name}. Задайте её перед запуском.")
    return value


def fetch_queue(bq: bigquery.Client, project: str) -> list[dict]:
    """Вся очередь по порядку, без вакансий, уже записанных в raw.digest_sent."""
    # Порядок задаёт dbt — колонка queue_position; здесь только сортируем по ней.
    #
    # not exists к raw.digest_sent, с любым status: витрина — таблица,
    # собранная dbt, и об исходах после сборки она не знает. Запусти скрипт
    # второй раз без пересборки — например, вручную после частичного сбоя, — и
    # без этого условия доставленные вакансии пришли бы повторно. С ним
    # повторный запуск возьмёт только то, что осталось.
    #
    # Всю очередь, а не limit MAX_PER_RUN в запросе: для шапки нужна её длина
    # («25 из 64»). В очереди десятки строк — прочитать их все проще, чем
    # писать второй запрос с count(*).
    #
    # f-строка здесь безопасна: в запрос подставляются только наши константы.
    query = f"""
        select
            d.vacancy_key,
            d.title,
            d.company_name,
            d.location_city,
            d.location_country,
            d.work_mode,
            d.seniority,
            d.stack,
            d.salary_min,
            d.salary_max,
            d.salary_currency,
            d.salary_period,
            d.residency_requirement,
            d.summary,
            d.url
        from `{project}.{MARTS_DATASET}.mart_digest_queue` as d
        where not exists (
            select 1
            from `{project}.{RAW_DATASET}.{TABLE_NAME}` as s
            where s.vacancy_key = d.vacancy_key
        )
        order by d.queue_position
    """
    return [dict(row) for row in bq.query(query).result()]


def first_sentences(text: str, count: int) -> str:
    """Первые count предложений текста.

    Граница предложения — точка, ! или ? и за ними пробел. Сокращение вроде
    «e.g. SQL» такое правило примет за конец предложения, и суть выйдет
    короче — но не сломается.
    """
    # (?<=[.!?]) — «перед этим местом стоит знак конца», сам знак остаётся
    # в предложении; \s+ — пробелы, по которым режем.
    sentences = re.split(r"(?<=[.!?])\s+", text.strip())
    return " ".join(sentences[:count])


def format_amount(value: float) -> str:
    """Число для зарплаты: 35000.0 → «35 000», 45.5 → «45.5»."""
    # BigQuery отдаёт FLOAT64, то есть 35000.0. Дробную часть показываем,
    # только если она есть: почасовая ставка бывает 45.5.
    if value.is_integer():
        return f"{value:,.0f}".replace(",", " ")
    return f"{value:,}".replace(",", " ")


def format_salary(vacancy: dict) -> str | None:
    """«45 000–55 000 EUR в год», «от 35 000 EUR»; None, если зарплаты нет."""
    low, high = vacancy["salary_min"], vacancy["salary_max"]
    if low is None and high is None:
        return None
    if low is not None and high is not None:
        amount = format_amount(low) if low == high else f"{format_amount(low)}–{format_amount(high)}"
    elif low is not None:
        amount = f"от {format_amount(low)}"
    else:
        amount = f"до {format_amount(high)}"
    # Валюты или периода может не быть — пустые части пропускаем.
    parts = [amount, vacancy["salary_currency"], SALARY_PERIOD_LABELS.get(vacancy["salary_period"])]
    return " ".join(part for part in parts if part)


def format_header(day: date, sending: int, in_queue: int) -> str:
    """Шапка: «вакансий: 10» или, если очередь длиннее лимита, «вакансий: 25 из 64, остальные завтра»."""
    if sending == in_queue:
        count = f"{sending}"
    else:
        count = f"{sending} из {in_queue}, остальные завтра"
    return f"<b>Дайджест за {day:%d.%m.%Y}</b>, вакансий: {count}"


def format_empty(day: date) -> str:
    """Единственное сообщение дня, когда очередь пуста."""
    return f"<b>Дайджест за {day:%d.%m.%Y}</b>: сегодня новых вакансий нет"


def format_vacancy(vacancy: dict) -> str:
    """Сообщение о вакансии в разметке HTML. Пустые поля не печатаются вовсе."""
    place = ", ".join(part for part in [vacancy["location_city"], vacancy["location_country"]] if part)
    seniority = vacancy["seniority"] if vacancy["seniority"] != "unclear" else None
    # stack у необогащённой вакансии — пустой список: так BigQuery отдаёт
    # null в REPEATED-колонке. or [] — страховка, если придёт None.
    stack = ", ".join(vacancy["stack"] or [])

    # Пары «подпись — значение». Значение None или пустая строка — строку
    # не печатаем: «Зарплата: —» только занимает место.
    fields = [
        ("Компания", vacancy["company_name"]),
        ("Город", place),
        ("Формат работы", WORK_MODE_LABELS.get(vacancy["work_mode"])),
        ("Грейд", seniority),
        ("Стек", stack),
        ("Зарплата", format_salary(vacancy)),
        ("Резидентство", vacancy["residency_requirement"]),
    ]

    # Всё, что пришло из данных, экранируем: parse_mode HTML, и символ < в
    # заголовке «Analyst <Remote>» Telegram принял бы за начало тега и
    # отказал бы во всём сообщении с кодом 400. В тексте экранируются < > &,
    # в адресе внутри href="..." — ещё и кавычки (quote=True), а & в ссылке
    # становится &amp;, и Telegram превращает его обратно.
    lines = [f"<b>{html.escape(vacancy['title'], quote=False)}</b>"]
    lines += [f"{label}: {html.escape(value, quote=False)}" for label, value in fields if value]
    if vacancy["summary"]:
        summary = first_sentences(vacancy["summary"], SUMMARY_SENTENCES)
        lines += ["", html.escape(summary, quote=False)]
    lines += ["", f'<a href="{html.escape(vacancy["url"], quote=True)}">Открыть вакансию</a>']
    return "\n".join(lines)


def telegram_answer(response: httpx.Response) -> dict:
    """Тело ответа Telegram; {} — если это не JSON (HTML-страница шлюза при 502)."""
    try:
        return response.json()
    except ValueError:
        return {}


def send_message(client: httpx.Client, chat_id: str, text: str) -> tuple[int | None, str | None]:
    """Отправляет одно сообщение; при временном сбое ждёт и повторяет.

    Возвращает два значения: код последнего ответа (None — ответа не было,
    сетевая ошибка) и описание сбоя (None — Telegram подтвердил доставку).
    Код нужен вызывающему, чтобы отличить 400 от остальных сбоев.
    """
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        # Без этого под каждой вакансией была бы большая карточка сайта.
        # disable_web_page_preview делал то же, но Telegram объявил его устаревшим.
        "link_preview_options": {"is_disabled": True},
    }
    attempts = len(RETRY_PAUSES) + 1
    for attempt in range(1, attempts + 1):
        code = None
        retry_after = 0
        try:
            response = client.post("sendMessage", json=payload)
        except (httpx.TimeoutException, httpx.ConnectError) as error:
            # Повторяем. Таймаут коварнее ошибки соединения: запрос мог дойти,
            # а потерялся только ответ, и тогда повтор пришлёт сообщение второй
            # раз. Дубль в чате лучше пропавшей вакансии.
            problem = f"сетевая ошибка {type(error).__name__}: {error}"
        except httpx.RequestError as error:
            # Прочие ошибки запроса (оборванное соединение и т. п.) не повторяем.
            return None, f"сетевая ошибка {type(error).__name__}: {error}"
        else:
            code = response.status_code
            answer = telegram_answer(response)
            if code == 200 and answer.get("ok"):
                return code, None
            # description — объяснение от Telegram, например
            # «Bad Request: can't parse entities: ...».
            problem = f"код {code}: {answer.get('description', 'ответ без описания')}"
            if code not in RETRY_STATUS_CODES:
                return code, problem
            # retry_after — сколько секунд Telegram просит подождать. Приходит с 429.
            retry_after = answer.get("parameters", {}).get("retry_after", 0)

        if attempt == attempts:
            return code, f"{problem}; не помогли {attempts} попытки"
        # attempt начинается с 1, а список пауз — с 0.
        pause = max(RETRY_PAUSES[attempt - 1], retry_after)
        print(f"  {problem}, попытка {attempt} из {attempts}, повтор через {pause} с", flush=True)
        time.sleep(pause)
    # Сюда не дойдём: последняя попытка всегда что-то возвращает.
    raise AssertionError("недостижимо")


def save_status(bq: bigquery.Client, table_id: str, vacancy_key: str, status: str) -> None:
    """Дописывает в raw.digest_sent окончательный исход по вакансии: sent или undeliverable."""
    # По строке сразу после исхода, а не всё в конце: упади скрипт на пятой
    # вакансии — четыре предыдущие уже записаны и из очереди уйдут.
    #
    # load job, как в enrich/with_gemini.py: бесплатен, строка сразу видна
    # запросам. Лимит — 1500 загрузок в таблицу в сутки, у нас до MAX_PER_RUN.
    # Только дописываем: таблица append-only (sql/raw_digest_sent.sql).
    job_config = bigquery.LoadJobConfig(
        schema=TABLE_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    row = {
        "vacancy_key": vacancy_key,
        "sent_at": datetime.now(timezone.utc).isoformat(),
        "status": status,
    }
    bq.load_table_from_json([row], table_id, job_config=job_config).result()


def send_digest() -> None:
    """Отправляет первые MAX_PER_RUN вакансий очереди, записывает исходы и печатает итог."""
    token = read_setting("TELEGRAM_BOT_TOKEN")
    chat_id = read_setting("TELEGRAM_CHAT_ID")
    project = os.environ["BQ_PROJECT"]        # упадёт с понятной ошибкой, если не задан

    bq = bigquery.Client(project=project)
    table_id = f"{project}.{RAW_DATASET}.{TABLE_NAME}"
    queue = fetch_queue(bq, project)
    # Срез ничего не выбрасывает: хвост очереди сегодня просто не отправляется.
    # Строк о нём в raw.digest_sent нет, и в следующий раз fetch_queue вернёт
    # его снова.
    batch = queue[:MAX_PER_RUN]
    today = datetime.now(TIMEZONE).date()
    print(f"в очереди: {len(queue)}, отправляем: {len(batch)}", flush=True)

    delivered = 0                          # сколько вакансий доставлено и записано как sent
    undeliverable: dict[str, str] = {}     # vacancy_key → отказ Telegram, записано как undeliverable
    failed: dict[str, str] = {}            # vacancy_key → причина сбоя, не записано

    # Токен — часть адреса: https://api.telegram.org/bot<токен>/sendMessage.
    # Адрес собираем один раз здесь и нигде не печатаем.
    with httpx.Client(
        base_url=f"https://api.telegram.org/bot{token}/",
        timeout=REQUEST_TIMEOUT_SECONDS,
    ) as client:
        if not queue:
            _, problem = send_message(client, chat_id, format_empty(today))
            if problem is not None:
                raise SystemExit(f"Очередь пуста, и сообщение об этом не ушло: {problem}")
            print("итог: очередь пуста, отправлено сообщение «сегодня новых вакансий нет»", flush=True)
            return

        # Шапка — такое же сообщение, как остальные: не ушла — пишем в лог
        # и отправляем вакансии дальше.
        _, header_problem = send_message(client, chat_id, format_header(today, len(batch), len(queue)))
        header_status = "доставлена" if header_problem is None else "не доставлена"
        print(f"шапка {header_status}" + (f": {header_problem}" if header_problem else ""), flush=True)
        time.sleep(PAUSE_SECONDS)

        # Сбой на одном сообщении не прерывает прогон. finally печатает итог,
        # даже если прогон упал.
        try:
            for number, vacancy in enumerate(batch, start=1):
                key = vacancy["vacancy_key"]
                progress = f"{number}/{len(batch)} {key}"
                code, problem = send_message(client, chat_id, format_vacancy(vacancy))

                # Исход → status для raw.digest_sent; None — не пишем ничего,
                # и вакансия остаётся в очереди.
                #
                # 400 считаем окончательным, только если шапка дошла. Шапка —
                # контрольное сообщение: текст у неё наш, без данных вакансий.
                # Дошла — значит токен, chat id и разметка в порядке, и 400 на
                # вакансии говорит о самом сообщении. Не дошла — 400 может
                # значить «chat not found» (опечатка в TELEGRAM_CHAT_ID), и
                # тогда недоставляемой навсегда стала бы вся очередь.
                if problem is None:
                    status = "sent"
                elif code == UNDELIVERABLE_STATUS_CODE and header_problem is None:
                    status = "undeliverable"
                else:
                    status = None

                if status is None:
                    failed[key] = problem
                    print(f"{progress} не доставлена, останется в очереди: {problem}", flush=True)
                else:
                    try:
                        save_status(bq, table_id, key, status)
                    except Exception:
                        # Исход есть, а записать не вышло — прогон останавливаем
                        # (raise ниже). Запись ломается обычно целиком: BigQuery
                        # недоступен, нет прав. Продолжи мы — все следующие
                        # вакансии тоже остались бы незаписанными и в следующий
                        # раз пришли бы повторно. Так повторится одна.
                        print(f"{progress} исход {status} не записан в {RAW_DATASET}.{TABLE_NAME} — "
                              f"вакансия останется в очереди", flush=True)
                        raise
                    if status == "sent":
                        delivered += 1
                        print(f"{progress} доставлена", flush=True)
                    else:
                        undeliverable[key] = problem
                        print(f"{progress} недоставляемая, убрана из очереди: {problem}", flush=True)

                # Пауза после каждого сообщения, включая последнее: условие
                # «кроме последнего» — лишняя ветка ради секунды.
                time.sleep(PAUSE_SECONDS)
        finally:
            # Осталось в очереди всё, у чего нет записи: хвост за лимитом и сбои.
            remaining = len(queue) - delivered - len(undeliverable)
            print(
                f"итог: в очереди {len(queue)}, отправляли {len(batch)}: доставлено {delivered}, "
                f"недоставляемых {len(undeliverable)}, сбоев {len(failed)}; шапка {header_status}; "
                f"осталось в очереди {remaining}",
                flush=True,
            )
            for undeliverable_key, reason in undeliverable.items():
                print(f"  недоставляемая: {undeliverable_key} — {reason}", flush=True)
            for failed_key, reason in failed.items():
                print(f"  сбой: {failed_key} — {reason}", flush=True)

    # Код 1 — только если не доставлена ни одна вакансия: тогда сломано что-то
    # общее (токен, chat id, сеть, разметка), а не отдельное сообщение.
    # Шапка без вакансий успехом не считается: «вакансий: 10» и дальше тишина —
    # та же поломка. Недоставляемые — тоже: до меня они не дошли.
    # Частичный успех — код 0, сбои видны в итоге выше.
    if not delivered:
        raise SystemExit(
            f"Не доставлена ни одна вакансия из {len(batch)}: причины — в итоге выше."
        )


if __name__ == "__main__":
    send_digest()
