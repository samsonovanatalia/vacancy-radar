-- Слой staging: те же данные, что в raw, только причёсанные.
-- Правило слоя: никакой бизнес-логики, только приведение к общему виду —
-- типы, регистр, переименование полей, отсев мусора.
--
-- Обратите внимание на две вещи, которых нет в обычном SQL:
--   {{ source('raw', 'arbeitnow') }} — ссылка на источник вместо имени таблицы.
--   Благодаря ей dbt строит граф зависимостей автоматически.
--
-- Источников теперь два, и у них слегка разный набор полей. Поэтому каждый
-- приводится к общему списку колонок в своём блоке, а потом они склеиваются
-- через union all. union all сопоставляет колонки ПО ПОРЯДКУ, а не по имени,
-- так что порядок и типы в обоих блоках должны совпадать до последней колонки.

with arbeitnow as (

    select
        source,

        -- source_id у arbeitnow — это slug, строка. У remoteok в raw лежит
        -- число (см. комментарий в блоке remoteok ниже). Приводим обе ветки
        -- к string явно, чтобы union all сходился по типам.
        cast(source_id as string)                   as source_id,

        title,
        company_name,
        location,
        remote,
        url,
        tags,
        job_types,
        description,
        created_at_unix,
        ingested_at,
        source_page,

        -- Зарплатной вилки arbeitnow не отдаёт. Пишем null, но обязательно
        -- с указанием типа: у голого null тип int64 по умолчанию, и на
        -- колонках вроде массивов union all упал бы на несовпадении типов.
        cast(null as int64)                         as salary_min,
        cast(null as int64)                         as salary_max

    from {{ source('raw', 'arbeitnow') }}

),

remoteok as (

    select
        source,

        -- В raw.remoteok эта колонка имеет тип int64: API отдаёт id то числом,
        -- то строкой, а схему таблицы BigQuery определил автоматически по
        -- первой загрузке. Раз raw мы не переписываем, чиним типы здесь.
        cast(source_id as string)                   as source_id,

        title,
        company_name,
        location,
        remote,
        url,
        tags,

        -- Этих двух полей у remoteok нет: тип массива строк указываем явно.
        cast(null as array<string>)                 as job_types,

        description,
        created_at_unix,
        ingested_at,
        cast(null as int64)                         as source_page,

        salary_min,
        salary_max

    from {{ source('raw', 'remoteok') }}

),

source_data as (

    -- union all, а не union distinct: дубли снимает блок deduplicated ниже,
    -- а union distinct заставил бы BigQuery сортировать все строки впустую
    -- (и вдобавок он не умеет работать с колонками-массивами).
    select * from arbeitnow

    union all

    select * from remoteok

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

        -- Зарплатная вилка есть только у remoteok, у arbeitnow здесь null.
        -- Колонку всё равно держим в staging: витрине важно отличать
        -- «зарплату не указали» от «источник её вообще не отдаёт».
        salary_min,
        salary_max,

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
