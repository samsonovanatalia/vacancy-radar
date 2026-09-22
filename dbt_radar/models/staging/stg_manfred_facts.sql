-- Структурные поля Manfred, которых нет у других источников: требуемые
-- языки с уровнем, процент удалёнки, список городов.
--
-- Зачем отдельная модель, а не колонки в stg_vacancies: там все источники
-- склеены через union all, и ради одного источника в каждую ветку пришлось
-- бы добавить пустые колонки. Здесь — только Manfred, а витрина
-- присоединяет эту модель по vacancy_key.
--
-- Правило слоя то же: никакой логики, только общий вид. Что значат эти
-- поля для подборки (100% удалёнки — это remote, какой язык требуют), решает
-- витрина mart_vacancies_scored.
--
-- ГРЕЙН: одна вакансия (vacancy_key). Коллектор каждый день дописывает
-- свежие строки, поэтому, как в stg_vacancies, оставляем самую свежую.

with manfred as (

    select
        concat(source, ':', cast(source_id as string))  as vacancy_key,

        -- Та же форма, что у llm_required_languages в ответе модели:
        -- массив записей {language, level}, код языка в нижнем регистре
        -- («EN» → «en»). Тогда в витрине поле источника и ответ модели
        -- взаимозаменяемы, и правило foreign_language_required читает
        -- любое из них без переделки.
        -- Пустой массив — компания языки не указала. Это НЕ «язык не
        -- нужен»: что с ним делать, решает витрина.
        array(
            select as struct
                lower(l.code)                           as language,
                l.level                                 as level
            from unnest(languages) as l
        )                                               as required_languages,

        -- Процент удалёнки как есть: 0–100.
        remote_percentage,

        -- Города по отдельности. Коллектор склеил список locations через
        -- «, », а каждый элемент сам вида «Город, Страна»: «Madrid, España,
        -- Barcelona, España». Разрезаем по «, » и берём чётные куски
        -- (0, 2, …) — это города. Пустой location (полная удалёнка) даёт
        -- пустой массив.
        array(
            select part
            from unnest(split(location, ', ')) as part with offset as position
            where mod(position, 2) = 0
            order by position
        )                                               as location_cities,

        ingested_at

    from {{ source('raw', 'manfred') }}

)

select
    vacancy_key,
    required_languages,
    remote_percentage,
    location_cities

from manfred

qualify row_number() over (
    partition by vacancy_key
    order by ingested_at desc
) = 1
