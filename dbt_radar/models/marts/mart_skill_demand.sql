-- Витрина: сколько вакансий в неделю упоминают каждый навык.
-- Это то, что будет главным графиком на дашборде.
--
-- ГРЕЙН (что означает одна строка): одна неделя × один навык.
-- Держите этот вопрос в голове для каждой витрины — на собеседовании
-- его задают первым, а ошибка в грейне ломает все цифры сверху.

with vacancies as (

    -- ref('имя_модели') в двойных фигурных скобках — ссылка на другую модель dbt.
    -- Так же, как source, только для таблиц, которые создаёт сам dbt.
    select * from {{ ref('stg_vacancies') }}

),

-- Пока навыки ищем простым поиском по тексту. На третьей неделе этот блок
-- заменит нормальный список навыков, который вернёт языковая модель.
-- Осознанно оставляем временное решение и помечаем его как временное.
skills as (

    select 'sql'        as skill union all
    select 'python'     union all
    select 'dbt'        union all
    select 'airflow'    union all
    select 'bigquery'   union all
    select 'snowflake'  union all
    select 'power bi'   union all
    select 'tableau'    union all
    select 'spark'

),

matched as (

    select
        date_trunc(date(v.posted_at), week)                      as week,
        s.skill,
        v.vacancy_key
    from vacancies as v
    cross join skills as s
    -- Ищем по description_clean, а не по description: в сыром описании
    -- arbeitnow — HTML, и навык находился даже в разметке. Например, «dbt»
    -- внутри картинки, вшитой в тег <img> строкой base64.
    where lower(v.description_clean) like concat('%', s.skill, '%')

)

select
    week,
    skill,
    count(distinct vacancy_key) as vacancies_count
from matched
group by week, skill
order by week desc, vacancies_count desc
