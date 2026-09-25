-- Витрина для сводки на дашборде: главные числа проекта.
--
-- ГРЕЙН: ровно одна строка, каждая метрика — отдельная колонка. В Looker
-- Studio так проще всего: одна карточка «Показатель» = одна колонка.
--
-- Почему строка всегда ровно одна: каждый блок ниже — агрегат без
-- group by, а такой запрос возвращает одну строку даже на пустой таблице
-- (count даёт 0, а не null). cross join трёх однострочных блоков — тоже
-- одна строка. Сторожит это тест assert_mart_dashboard_kpi_one_row.
--
-- Периоды у метрик разные, и это надо помнить, глядя на карточки:
--   - первые четыре — из mart_vacancies_scored, то есть только свежие
--     вакансии: опубликованы за 30 дней, у Manfred — видели в списке
--     за 2 дня;
--   - sources_active — за 7 дней по дате сбора (ingested_at);
--   - sent_total — за всё время работы бота.

with scored as (

    select
        count(*)                                        as vacancies_fresh,
        -- Рыночная — дата-роль (role_type не other), не продажи и не
        -- найм по заголовку, не микрозадачи для обучения ИИ и не дубль.
        countif(is_market_vacancy)                      as market_vacancies,
        -- null в excluded_reason — вакансию не исключило ни одно правило,
        -- то же условие, что в mart_vacancies_for_me.
        countif(excluded_reason is null)                as matching_vacancies,
        -- Вилка указана, если есть хотя бы одна граница: так же считает
        -- mart_salary. _best — поле источника, а если его нет — ответ модели.
        -- Только евро за год — то же население, что таблица зарплат на
        -- дашборде (mart_salary_facts с фильтрами EUR и year). Без этого
        -- сводка считала и GBP, и месячные вилки, и два числа не сходились.
        -- Значения в верхнем и нижнем регистре, как их пишет
        -- mart_vacancies_scored: валюта — ISO 4217, период — строчными.
        countif(
            is_market_vacancy
            and (salary_min_best is not null or salary_max_best is not null)
            and salary_currency_best = 'EUR'
            and salary_period_best = 'year'
        )                                               as with_salary_eur_year
    from {{ ref('mart_vacancies_scored') }}

),

sources as (

    -- Из stg_vacancies, а не из scored: в scored окно по дате публикации,
    -- а здесь вопрос «работает ли сбор». ingested_at в stg_vacancies —
    -- последний раз, когда вакансия пришла из источника.
    select
        count(distinct source)                          as sources_active
    from {{ ref('stg_vacancies') }}
    where ingested_at >= timestamp_sub(current_timestamp(), interval 7 day)

),

sent as (

    -- Только доставленные: undeliverable — Telegram отказал, до меня
    -- вакансия не дошла. stg_digest_sent — одна строка на вакансию, так что
    -- повторная запись бота не посчитается дважды.
    select
        countif(status = 'sent')                        as sent_total
    from {{ ref('stg_digest_sent') }}

)

select
    scored.vacancies_fresh,
    scored.market_vacancies,
    scored.matching_vacancies,
    scored.with_salary_eur_year,
    sources.sources_active,
    sent.sent_total
from scored
cross join sources
cross join sent
