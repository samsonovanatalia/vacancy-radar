-- Пропущенные дни конвейера: за последние 14 полных дней — был ли сбор и
-- была ли отправка.
--
-- ГРЕЙН: один день (run_date) по Барселоне. Строка есть на каждый день окна,
-- даже если в тот день не пришло ни одной строки, — иначе пропущенный день
-- просто исчез бы из результата, а увидеть надо именно его. Поэтому дни
-- берутся из generate_date_array, а не из самих данных.
--
-- Окно — 14 дней, кончается вчера. Сегодняшний день не полный: сбор и
-- дайджест сейчас приходят днём, и до них сегодня выглядел бы пропуском.
--
-- view, а не table, как остальные marts: таблицу пересобирает только
-- collect.yml, и если сбор не запустился, она осталась бы вчерашней — ровно
-- в тот день, когда нужна. view считается заново при каждом чтении:
-- дашборд и тест видят текущее состояние raw, даже без сборки.
--
-- Сбор — строки в raw-таблицах активных источников (как в collect.yml):
-- arbeitnow, remoteok, adzuna, manfred. Отключённые remotive и careerjet не
-- считаем: у них пусто каждый день, и это не пропуск.
--
-- Дайджест — запуск в raw.digest_runs со status sent или empty: ушли вакансии
-- или сообщение «новых вакансий нет». failed и skipped дайджестом не считаем:
-- первый ничего не доставил, второй вышел, потому что дайджест уже был, — и
-- тот, настоящий, найдётся в той же таблице.
--
-- Переходный период. raw.digest_runs ведётся с 19.10.2026; за дни до её
-- первой строки запусков в ней нет, и они все выглядели бы пропусками. Для
-- таких дней дайджест ищем по-старому — строки в raw.digest_sent; какой
-- таблицей ответили, видно в digest_basis. С 03.11.2026 все 14 дней окна
-- будут после первой строки, и ветку digest_sent можно убрать.
--
-- Читаем raw напрямую, а не staging: в stg_vacancies дубли схлопнуты по
-- последнему ingested_at, и день, чьи строки совпали с завтрашними, пропал бы.
-- Фильтр по ingested_at — по колонке партиции, BigQuery читает только 15 дней.
--
-- Тест tests/assert_no_missed_pipeline_days.sql выводит пропуски в лог
-- каждого dbt build: до трёх пропусков — warn, больше — error.

{{ config(materialized='view') }}

-- «Сегодня» по Барселоне — одним выражением на всю модель. Переменная нужна
-- unit-тесту в _marts.yml: он подставляет постоянную дату, чтобы проверять
-- выдуманные строки, не зависящие от дня запуска. В обычной сборке её нет,
-- и берётся current_date.
{% set today = var('pipeline_today', "current_date('Europe/Madrid')") %}

with days as (

    select run_date
    from unnest(generate_date_array(
        date_sub({{ today }}, interval 14 day),
        date_sub({{ today }}, interval 1 day)
    )) as run_date

),

collected as (

    select 'arbeitnow' as source, date(ingested_at, 'Europe/Madrid') as run_date
    from {{ source('raw', 'arbeitnow') }}
    where ingested_at >= timestamp(date_sub({{ today }}, interval 15 day))

    union all

    select 'remoteok', date(ingested_at, 'Europe/Madrid')
    from {{ source('raw', 'remoteok') }}
    where ingested_at >= timestamp(date_sub({{ today }}, interval 15 day))

    union all

    select 'adzuna', date(ingested_at, 'Europe/Madrid')
    from {{ source('raw', 'adzuna') }}
    where ingested_at >= timestamp(date_sub({{ today }}, interval 15 day))

    union all

    select 'manfred', date(ingested_at, 'Europe/Madrid')
    from {{ source('raw', 'manfred') }}
    where ingested_at >= timestamp(date_sub({{ today }}, interval 15 day))

),

collected_by_day as (

    select
        run_date,
        count(*)                                    as collected_rows,
        count(distinct source)                      as sources_collected
    from collected
    group by run_date

),

-- Успешные запуски дайджеста по дням. Фильтр по run_date — по колонке
-- партиции.
runs_by_day as (

    select
        run_date,
        sum(sent_count)                             as vacancies_sent
    from {{ source('raw', 'digest_runs') }}
    where run_date >= date_sub({{ today }}, interval 15 day)
      and status in ('sent', 'empty')
    group by run_date

),

-- С какого дня ведётся raw.digest_runs — с любым status. Таблица пуста —
-- null, и тогда все дни окна идут по digest_sent.
runs_start as (

    select min(run_date)                            as first_run_date
    from {{ source('raw', 'digest_runs') }}

),

-- Переходный период: доставленные вакансии по дням из raw.digest_sent.
sent_by_day as (

    select
        date(sent_at, 'Europe/Madrid')              as run_date,
        count(*)                                    as vacancies_sent
    from {{ source('raw', 'digest_sent') }}
    where sent_at >= timestamp(date_sub({{ today }}, interval 15 day))
    group by run_date

),

days_with_basis as (

    select
        days.run_date,
        -- coalesce(..., false): runs_start пуста — первого дня нет, сравнение
        -- даёт null, а null здесь значит «ещё digest_sent».
        if(coalesce(days.run_date >= runs_start.first_run_date, false),
           'digest_runs', 'digest_sent')            as digest_basis
    from days
    cross join runs_start

)

select
    days_with_basis.run_date,
    coalesce(collected_by_day.collected_rows, 0)    as collected_rows,
    coalesce(collected_by_day.sources_collected, 0) as sources_collected,
    days_with_basis.digest_basis,
    coalesce(
        if(days_with_basis.digest_basis = 'digest_runs',
           runs_by_day.vacancies_sent, sent_by_day.vacancies_sent),
        0
    )                                               as vacancies_sent,
    collected_by_day.run_date is null               as is_collect_missing,
    if(days_with_basis.digest_basis = 'digest_runs',
       runs_by_day.run_date is null,
       sent_by_day.run_date is null)                as is_digest_missing
from days_with_basis
left join collected_by_day
    on collected_by_day.run_date = days_with_basis.run_date
left join runs_by_day
    on runs_by_day.run_date = days_with_basis.run_date
left join sent_by_day
    on sent_by_day.run_date = days_with_basis.run_date
