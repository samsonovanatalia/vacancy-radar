"""
Отправка вакансий из очереди в Telegram.

Что делает скрипт:
  1. Читает очередь mart_digest_queue — все подходящие вакансии, у которых
     ещё нет исхода отправки, — без тех, что уже записаны в raw.digest_sent
     (см. fetch_queue).
  2. Берёт первые MAX_PER_RUN по queue_position (в догоняющем прогоне — всю
     очередь, см. ниже). Отправляет шапку «Дайджест за <дата>, вакансий: N» —
     или «25 из 64, остальные завтра», если очередь длиннее, — затем по одному
     сообщению на вакансию. Очередь пуста — одно сообщение «сегодня новых
     вакансий нет»: молчание бота не должно выглядеть так же, как поломка.
  3. После каждой вакансии с окончательным исходом дописывает строку в
     raw.digest_sent: vacancy_key, время и status:
       sent          — Telegram подтвердил доставку;
       undeliverable — Telegram отказал с кодом 400: такое сообщение не уйдёт
                       и завтра, а без записи вакансия вечно занимала бы
                       очередь.
     Временный сбой (429, 5xx, сеть) не пишется: вакансия остаётся в очереди
     и уйдёт в следующий раз. Следующая сборка dbt уберёт из очереди всё
     записанное (stg_digest_sent → mart_digest_queue).
  4. Печатает, когда истекает raw.digest_sent в песочнице BigQuery, и
     пересоздаёт её, если осталось меньше RENEW_BEFORE (см. keep_table_alive), —
     в любой день, даже когда очередь пуста или не доставлено ничего.

Почему лимит, а не вся очередь сразу: очередь копит вакансии за 30 дней, и
сотня сообщений подряд — уже не подборка, а лента. Лимит откладывает, а не
выбрасывает: вакансия, не вошедшая в первые MAX_PER_RUN, ничего не
записывает и остаётся в очереди. В этом отличие от прежнего топ-10, где
одиннадцатая вакансия не приходила никогда.

Догоняющий прогон (--catch-up) — разовый: когда очередь накопилась, например
после расширения окна, и ждать, пока она разойдётся по MAX_PER_RUN в день, не
хочется. Поднять лимит выше MAX_PER_RUN можно только этим флагом, а число в
командной строке его только опускает — как в enrich/with_gemini.py и
fetch/adzuna_pages.py. Так опечатка «250» вместо «25» не отправит в чат всю
очередь: всю очередь отправляет только явно названный режим.

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
    python -m notify.telegram                  # первые MAX_PER_RUN вакансий очереди
    python -m notify.telegram 5                # пробный прогон: первые 5
    python -m notify.telegram --catch-up       # догоняющий: вся очередь целиком
    python -m notify.telegram 40 --catch-up    # догоняющий, но не больше 40
"""

from __future__ import annotations

import argparse
import html
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import httpx
from google.cloud import bigquery

MARTS_DATASET = "dbt_natalia_marts"
RAW_DATASET = "raw"
TABLE_NAME = "digest_sent"

# Сколько вакансий отправлять за обычный запуск. Это не дневная норма и не
# «столько вакансий в день мне нужно»: в обычный день очередь короче сотни, и
# лимит вообще не срабатывает. Это предохранитель на случай, когда фильтр
# сломался и в очередь провалилось всё подряд, — чтобы в чат ушла сотня
# сообщений, а не тысяча, и чтобы поломку было видно по шапке.
#
# Лимит откладывает, а не выбрасывает: вакансия, не попавшая в первые
# MAX_PER_RUN по queue_position, ничего не записывает в digest_sent и остаётся
# в очереди — остаток уйдёт следующим прогоном. Снимает этот потолок только
# флаг --catch-up (см. batch_limit).
MAX_PER_RUN = 100

# Дата в шапке — по Барселоне, а не по часам машины, на которой идёт скрипт:
# на раннере GitHub часы в UTC, а дата должна совпадать с календарём читателя.
TIMEZONE = ZoneInfo("Europe/Madrid")

# Таймаут одного запроса к Telegram, секунды.
REQUEST_TIMEOUT_SECONDS = 30

# Пауза после каждого сообщения, секунды. Telegram просит не отправлять в
# один чат чаще одного сообщения в секунду: короткий всплеск сойдёт, а на
# длинной серии — например, в догоняющем прогоне на полсотни вакансий —
# начнутся ответы 429. Их мы переждём (см. send_message), но лучше до них не
# доводить: 1.5 секунды — лимит с запасом.
#
# На деле промежуток между вакансиями длиннее: после каждой доставленной
# идёт запись в BigQuery, это ещё пара секунд. Но на скорость BigQuery не
# полагаемся — темп держит сама пауза.
PAUSE_SECONDS = 1.5

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

# Язык объявления (ISO 639-1 из ответа модели) → слово для строки
# «Объявление на испанском». Только те языки, что встречаются в наших
# источниках; незнакомый код печатается как есть — см. format_language.
LANGUAGE_LABELS = {
    "es": "испанском",
    "ca": "каталанском",
    "de": "немецком",
    "fr": "французском",
    "nl": "нидерландском",
    "pt": "португальском",
    "it": "итальянском",
    "pl": "польском",
    "ru": "русском",
}

# За сколько дней до истечения пересоздавать raw.digest_sent.
#
# Проект в песочнице BigQuery: у таблицы без партиций срок жизни — 60 дней от
# СОЗДАНИЯ, потом она удаляется целиком. Для digest_sent это значило бы:
# журнал отправок пропал, и бот заново прислал бы всю подборку.
#
# Сдвинуть срок нельзя: песочница не даёт поставить больше «создание + 61
# день» (проверено 2026-09-24, отказ 403 «Table expiration time must be less
# than 60 days while in sandbox mode»). Работает пересоздание: create or
# replace из самой себя даёт таблице новую дату создания, а с ней новые 60
# дней; строки и схема сохраняются, замена атомарна.
#
# Пересоздаём не каждый день, а когда осталось меньше RENEW_BEFORE: это
# единственный журнал отправок, и чем реже его переписывать целиком, тем
# меньше поводов что-то сломать. 30 дней — половина срока: даже месяц
# упавших прогонов подряд таблицу не погубит.
#
# Почему не партиции, как у остальных raw-таблиц: у них удаляются партиции
# старше 60 дней, и вакансия Manfred, которая висит в списке дольше, пришла
# бы повторно — её запись об отправке уже удалена. Почему не биллинг: решено
# держать проект бесплатным.
RENEW_BEFORE = timedelta(days=30)

# Схема явная, как в остальных модулях, и та же, что в sql/raw_digest_sent.sql.
TABLE_SCHEMA = [
    bigquery.SchemaField("vacancy_key", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("sent_at", "TIMESTAMP", mode="REQUIRED"),
    # Необязательная, хотя бот пишет status всегда: в таблице колонка
    # необязательная (см. sql/raw_digest_sent.sql), а схема загрузки должна
    # совпадать со схемой таблицы.
    bigquery.SchemaField("status", "STRING"),
]


def read_args() -> argparse.Namespace:
    """Разбирает командную строку: необязательное число вакансий и флаг --catch-up."""
    parser = argparse.ArgumentParser(description="Отправка очереди вакансий в Telegram.")
    # nargs="?" — число необязательно. Без него default=None, и сколько
    # отправлять, решает batch_limit: MAX_PER_RUN или, с --catch-up, всю очередь.
    parser.add_argument(
        "limit", nargs="?", type=int, default=None,
        help=f"сколько вакансий отправить; без --catch-up не больше {MAX_PER_RUN}",
    )
    # store_true — флаг без значения: есть в командной строке — True, нет —
    # False. Имя атрибута argparse делает сам: --catch-up → catch_up.
    parser.add_argument(
        "--catch-up", action="store_true",
        help=f"догоняющий прогон: снять потолок {MAX_PER_RUN} и отправить всю очередь",
    )
    args = parser.parse_args()
    # int("-3") разбирается без ошибки, поэтому ноль и минус ловим отдельно.
    if args.limit is not None and args.limit < 1:
        parser.error("число вакансий должно быть больше нуля")
    return args


def batch_limit(limit: int | None, catch_up: bool) -> int | None:
    """Сколько вакансий очереди отправить за этот запуск; None — всю очередь.

    Число опускает лимит всегда, поднимает выше MAX_PER_RUN — только вместе
    с --catch-up:
        limit   catch_up   итог
        —       нет        MAX_PER_RUN
        5       нет        5
        40      нет        MAX_PER_RUN
        —       да         None, вся очередь
        40      да         40
    """
    # Отдельная функция, а не три строки в send_digest: правило целиком видно
    # в одном месте, и тест проверяет его таблицей, без Telegram и BigQuery.
    if catch_up:
        return limit
    if limit is None:
        return MAX_PER_RUN
    return min(limit, MAX_PER_RUN)


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
    # Всю очередь, а не limit в запросе: для шапки нужна её длина («25 из 64»).
    # В очереди десятки строк — прочитать их все проще, чем писать второй
    # запрос с count(*).
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
            d.language,
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


def format_language(code: str | None) -> str | None:
    """«Объявление на испанском» для неанглийского объявления; None для английского или неизвестного."""
    # С 2026-09-22 в подборку проходят объявления на любом языке, если
    # модель не нашла требования чужого языка. Пометка — чтобы открыть
    # ссылку и не удивиться испанскому тексту.
    # None — вакансия не обогащена: язык неизвестен, молчим, а не гадаем.
    if not code or code == "en":
        return None
    label = LANGUAGE_LABELS.get(code)
    return f"Объявление на {label}" if label else f"Объявление не на английском ({code})"


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
    language = format_language(vacancy["language"])
    if language:
        # Код языка пришёл от модели — экранируем, как всё из данных.
        lines.append(html.escape(language, quote=False))
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
    # запросам. Лимит — 1500 загрузок в таблицу в сутки; даже догоняющий
    # прогон по всей очереди до него далеко не дотягивает.
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


def keep_table_alive(bq: bigquery.Client, table_id: str) -> None:
    """Печатает срок жизни raw.digest_sent; если осталось меньше RENEW_BEFORE — пересоздаёт её.

    Строка со сроком печатается в каждом прогоне: продление должно быть
    видно. Если однажды оно тихо перестанет работать, по логу это заметно
    задолго до удаления таблицы. Ошибку не глотаем: не вышло — прогон
    падает и задача краснеет, хотя сообщения к этому моменту уже отправлены.
    """
    table = bq.get_table(table_id)
    name = f"{RAW_DATASET}.{TABLE_NAME}"
    if table.expires is None:
        # Вне песочницы (подключили биллинг) срока может не быть вовсе.
        print(f"{name}: срока жизни нет, пересоздавать не нужно", flush=True)
        return

    left = table.expires - datetime.now(timezone.utc)
    print(f"{name}: истекает {table.expires:%Y-%m-%d %H:%M} UTC, осталось {left.days} дн.", flush=True)
    if left >= RENEW_BEFORE:
        return

    rows_before = table.num_rows
    # Колонки перечислены явно, с not null: create ... as select без списка
    # сделал бы все колонки необязательными, и схема разошлась бы с
    # sql/raw_digest_sent.sql и TABLE_SCHEMA. Меняешь схему там — поменяй и здесь.
    bq.query(f"""
        create or replace table `{table_id}` (
            vacancy_key string not null,
            sent_at     timestamp not null,
            status      string
        ) as
        select vacancy_key, sent_at, status
        from `{table_id}`
    """).result()

    table = bq.get_table(table_id)
    # Это единственный журнал отправок: потерять из него строку — значит
    # прислать вакансию повторно. Замена атомарна, и расхождения быть не
    # должно; если оно всё же есть — падаем громко, а не идём дальше.
    if table.num_rows != rows_before:
        raise RuntimeError(
            f"{name}: после пересоздания строк {table.num_rows}, а было {rows_before}"
        )
    print(f"{name}: пересоздана, строк {table.num_rows}, теперь истекает "
          f"{table.expires:%Y-%m-%d %H:%M} UTC", flush=True)


def send_digest(limit: int | None, catch_up: bool) -> None:
    """Отправляет вакансии очереди, записывает исходы и печатает итог.

    limit — число из командной строки или None; catch_up — флаг --catch-up.
    Сколько отправить на самом деле, решает batch_limit.
    """
    token = read_setting("TELEGRAM_BOT_TOKEN")
    chat_id = read_setting("TELEGRAM_CHAT_ID")
    project = os.environ["BQ_PROJECT"]        # упадёт с понятной ошибкой, если не задан

    bq = bigquery.Client(project=project)
    table_id = f"{project}.{RAW_DATASET}.{TABLE_NAME}"
    queue = fetch_queue(bq, project)
    # Срез ничего не выбрасывает: хвост очереди сегодня просто не отправляется.
    # Строк о нём в raw.digest_sent нет, и в следующий раз fetch_queue вернёт
    # его снова. queue[:None] — вся очередь: срез без конца идёт до конца списка.
    batch = queue[:batch_limit(limit, catch_up)]
    today = datetime.now(TIMEZONE).date()

    if not catch_up and limit is not None and limit > MAX_PER_RUN:
        # Молча урезать нельзя: человек попросил 40, получил 25 и не понял бы почему.
        print(f"число {limit} больше MAX_PER_RUN = {MAX_PER_RUN}, отправляем не больше "
              f"{MAX_PER_RUN}: поднять потолок можно только флагом --catch-up", flush=True)
    if catch_up:
        # Отдельной строкой: по логу сразу видно, что прогон был необычный и
        # сколько сообщений он собирается отправить.
        print(f"догоняющий прогон: потолок MAX_PER_RUN = {MAX_PER_RUN} снят, "
              f"отправляем {len(batch)} из {len(queue)}", flush=True)
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
            # Записей сегодня нет, а следить за сроком всё равно нужно:
            # иначе несколько тихих недель подряд — и таблицу удалят.
            keep_table_alive(bq, table_id)
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
                # «кроме последнего» — лишняя ветка ради полутора секунд.
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

    # После всех записей и до проверки ниже: в день, когда не доставлено
    # ничего, за сроком тоже надо проследить. Сюда не доходим, только если
    # прогон упал с исключением, — тогда проверит следующий: пересоздаём
    # за RENEW_BEFORE до истечения, запас большой.
    keep_table_alive(bq, table_id)

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
    args = read_args()
    send_digest(args.limit, args.catch_up)
