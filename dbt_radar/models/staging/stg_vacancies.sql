-- Слой staging: те же данные, что в raw, только причёсанные.
-- Правило слоя: никакой бизнес-логики, только приведение к общему виду —
-- типы, регистр, переименование полей, отсев мусора.
--
-- Обратите внимание на две вещи, которых нет в обычном SQL:
--   {{ source('raw', 'arbeitnow') }} — ссылка на источник вместо имени таблицы.
--   Благодаря ей dbt строит граф зависимостей автоматически.

with source_data as (

    select * from {{ source('raw', 'arbeitnow') }}

),

cleaned as (

    select
        -- Ключ вакансии: источник + его собственный id.
        -- Одинаковый slug у разных источников — не редкость, поэтому склеиваем.
        concat(source, ':', source_id)              as vacancy_key,
        source,
        source_id,

        trim(title)                                 as title,
        lower(trim(title))                          as title_normalized,
        trim(company_name)                          as company_name,
        trim(location)                              as location,

        -- В источнике remote приходит как true/false, но у других источников
        -- это будет строка. Приводим к одному типу здесь, а не в витрине.
        cast(remote as bool)                        as is_remote,

        url,
        tags,
        job_types,
        description,

        -- unix-время → нормальная временная метка
        timestamp_seconds(created_at_unix)          as posted_at,
        cast(ingested_at as timestamp)              as ingested_at

    from source_data

    -- Мусорные строки без заголовка нам не нужны нигде дальше.
    where title is not null

),

deduplicated as (

    select *
    from cleaned
    qualify row_number() over (
        partition by vacancy_key
        order by ingested_at desc
    ) = 1

)

select * from deduplicated

