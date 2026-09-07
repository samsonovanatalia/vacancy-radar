-- Слой staging: те же данные, что в raw, только причёсанные.
-- Правило слоя: никакой бизнес-логики, только приведение к общему виду —
-- типы, регистр, переименование полей, отсев мусора.
--
-- Обратите внимание на две вещи, которых нет в обычном SQL:
--   {{ source('raw', 'arbeitnow') }} — ссылка на источник вместо имени таблицы.
--   Благодаря ей dbt строит граф зависимостей автоматически.
--
-- Источников теперь четыре, и у них слегка разный набор полей. Поэтому каждый
-- приводится к общему списку колонок в своём блоке, а потом они склеиваются
-- через union all. union all сопоставляет колонки ПО ПОРЯДКУ, а не по имени,
-- так что порядок и типы во ВСЕХ блоках должны совпадать до последней колонки.

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
        cast(null as int64)                         as salary_max,

        -- Зарплату текстом отдаёт только remotive. Здесь null, но снова
        -- с явным типом: иначе int64 по умолчанию не сойдётся со string.
        cast(null as string)                        as salary_text

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
        salary_max,
        cast(null as string)                        as salary_text

    from {{ source('raw', 'remoteok') }}

),

remotive as (

    select
        source,

        -- Здесь коллектор уже привёл id к строке на входе, но cast оставляем:
        -- он ничего не стоит и защищает от сюрприза, если BigQuery когда-нибудь
        -- определит тип колонки иначе.
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

        -- Постраничного обхода у remotive нет — весь список приходит разом.
        cast(null as int64)                         as source_page,

        -- Внимание, тонкость. В raw_remotive.jsonl колонки salary_min и
        -- salary_max есть, но во ВСЕХ строках они null. BigQuery определяет
        -- схему по данным и колонку, где нет ни одного значения, просто
        -- не создаёт. Поэтому ссылаться на них по имени нельзя — модель
        -- упала бы с "Unrecognized name". Пишем null явным типом.
        cast(null as int64)                         as salary_min,
        cast(null as int64)                         as salary_max,

        -- А вот это у remotive единственного и есть: "$50-$75 /hour",
        -- "$20k -$35k". Разбирать текст в числа здесь не будем — staging
        -- только причёсывает, парсинг зарплат это бизнес-логика для marts.
        salary_text

    from {{ source('raw', 'remotive') }}

),

adzuna as (

    select
        source,

        -- Не косметика: в raw.adzuna эта колонка имеет тип INTEGER.
        -- Коллектор пишет строку "5874122665", но автодетект схемы видит
        -- в кавычках одни цифры и решает, что это число. Возвращаем string,
        -- иначе union all не сойдётся по типам с остальными ветками.
        cast(source_id as string)                   as source_id,

        title,
        company_name,
        location,

        -- Adzuna не говорит, удалённая вакансия или нет: коллектор честно
        -- кладёт None, и колонка пустая во всех строках. BigQuery дал ей
        -- тип STRING (по пустоте угадать нечего), а нам нужен bool.
        -- Правило для этой модели: если источник поля НЕ ОТДАЁТ вовсе —
        -- пишем типизированный null, не глядя на то, что там в raw.
        -- Так ветка не сломается и после пересоздания сырой таблицы.
        cast(null as bool)                          as remote,

        url,

        -- Тегов у Adzuna нет, коллектор пишет пустой список. Тот же приём.
        cast(null as array<string>)                 as tags,

        -- А вот job_types наполнен (13 строк из 413 в первой загрузке):
        -- туда идёт contract_type — "permanent" или "contract".
        job_types,

        description,
        created_at_unix,
        ingested_at,

        -- Постраничного обхода нет: берём одну страницу на запрос.
        cast(null as int64)                         as source_page,

        -- Зарплата. В raw сюда попадает только настоящая вилка из
        -- объявления (19 строк из 413 в первой загрузке): собственный
        -- прогноз Adzuna коллектор отсекает по флагу salary_is_predicted.
        -- Adzuna документирует эти поля как дробные числа, и если в
        -- какой-то день приедет 45000.5, автодетект сделает колонку
        -- FLOAT64. cast к int64 стоит ноль и держит тип стабильным.
        cast(salary_min as int64)                   as salary_min,
        cast(salary_max as int64)                   as salary_max,

        -- Зарплату текстом отдаёт только remotive.
        cast(null as string)                        as salary_text

    from {{ source('raw', 'adzuna') }}

),

source_data as (

    -- union all, а не union distinct: дубли снимает блок deduplicated ниже,
    -- а union distinct заставил бы BigQuery сортировать все строки впустую
    -- (и вдобавок он не умеет работать с колонками-массивами).
    select * from arbeitnow

    union all

    select * from remoteok

    union all

    select * from remotive

    union all

    select * from adzuna

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

        -- Зарплатная вилка числами есть у remoteok и adzuna, у остальных null.
        -- Колонку всё равно держим в staging: витрине важно отличать
        -- «зарплату не указали» от «источник её вообще не отдаёт».
        salary_min,
        salary_max,

        -- Зарплата словами, как её написал работодатель (только remotive).
        -- Тащим её дальше как есть: даже без чисел это полезно и боту,
        -- и глазами — а разобрать текст в вилку можно будет позже,
        -- не перезагружая сырой слой.
        salary_text,

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
