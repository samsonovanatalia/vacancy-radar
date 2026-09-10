-- Витрина: вакансии, отобранные под мой профиль. То, что бот отправит в 12:00.
--
-- Теперь это тонкий слой поверх mart_vacancies_scored. Вся логика — признаки,
-- дубли, баллы — живёт там, а здесь осталось одно правило: показывать только
-- то, что не исключило ни одно правило (excluded_reason is null).
--
-- ГРЕЙН: одна вакансия. Только фильтр, строк не добавляем — уникальность
-- vacancy_key наследуется от scored.
--
-- Почему view, а не table: view не хранит данные, а при каждом обращении
-- читает свежую mart_vacancies_scored. Таблица хранила бы копию тех же строк,
-- которую надо пересобирать строго после scored. Дорогие регулярки уже
-- посчитаны в таблице scored, так что запрос к view дешёвый: фильтр + сортировка.
-- config здесь перекрывает +materialized: table из dbt_project.yml.

{{ config(materialized='view') }}

select
    vacancy_key,
    source,
    posted_at,
    title,
    company_name,
    location,
    is_remote,
    salary_min,
    salary_max,
    salary_text,
    url,
    relevance_score

from {{ ref('mart_vacancies_scored') }}

where excluded_reason is null

-- Оговорка остаётся прежней: порядок строк при чтении без order by ничего
-- не гарантирует. Сортировка — для глаз при select из витрины; боту всё
-- равно нужен свой order by.
order by relevance_score desc, posted_at desc
