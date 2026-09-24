-- Витрина для дашборда: спрос на навыки по неделям и ролям.
--
-- ГРЕЙН (что означает одна строка): одна неделя × один навык × одна роль.
-- Держите этот вопрос в голове для каждой витрины — на собеседовании
-- его задают первым, а ошибка в грейне ломает все цифры сверху.
--
-- НАСЕЛЕНИЕ — весь рынок дата-ролей (is_market_vacancy), а не подборка.
-- Подборка отвечает «куда я могу откликнуться», дашборд — «чего хочет
-- рынок». Фильтры по грейду, языку, локации и отправке здесь измеряли бы
-- наш фильтр, а не рынок. Дубли схлопнуты: дубль — та же вакансия.
--
-- НАВЫКИ — из поля stack ответа модели, а не поиском подстроки в тексте
-- (до 2026-09-24). Подстрока ловила «Sparkassen» как spark, «NoSQL» как
-- sql, а по обрывку описания Adzuna в 500 символов недосчитывала: на одних
-- и тех же 350 вакансиях sql 172 по тексту против 268 по stack.
-- Написания одного инструмента склеивает макрос normalize_skill.
--
-- ОГРАНИЧЕНИЯ, которые надо помнить, глядя на график:
--   - витрина строится из mart_vacancies_scored, а там окно в 30 дней:
--     старые недели выпадают, это скользящий срез, а не история;
--   - stack есть только у обогащённых вакансий. Сколько их в каждой
--     неделе и роли — колонки role_vacancies_count и
--     role_vacancies_with_stack; доля навыка = vacancies_count /
--     role_vacancies_with_stack;
--   - неделя — по дате публикации, у Manfred — по первому появлению в
--     списке (first_seen_at): его posted_at — это updatedAt.

with market as (

    select
        s.vacancy_key,
        s.role_type,
        -- isoweek — неделя с понедельника, как в Испании. Обычный week в
        -- BigQuery начинается с воскресенья.
        date_trunc(date(coalesce(m.first_seen_at, s.posted_at)), isoweek) as week,
        -- coalesce: признаки бывают null (нет ответа модели), а в countif
        -- ниже null — это не «нет», а выпавшая строка. Нужен честный false.
        coalesce(s.is_barcelona or s.location_fit = 'spain', false)      as is_spain,
        coalesce(s.work_mode = 'remote' or s.location_fit = 'remote', false)
                                                                          as is_remote,
        e.stack

    from {{ ref('mart_vacancies_scored') }} as s
    -- left join: у вакансий других источников строки в stg_manfred_facts
    -- нет, а у необогащённых — строки в stg_enrichment. Обычный join
    -- выбросил бы их из знаменателя.
    left join {{ ref('stg_manfred_facts') }} as m using (vacancy_key)
    left join {{ ref('stg_enrichment') }} as e using (vacancy_key)
    where s.is_market_vacancy

),

role_week as (

    -- Знаменатели: сколько рыночных вакансий в неделе и роли всего и у
    -- скольких из них есть stack. Считаются отдельно, до разворота stack:
    -- вакансия без навыков в развёрнутые строки не попадёт вовсе.
    select
        week,
        role_type,
        count(*)                                        as role_vacancies_count,
        countif(array_length(stack) > 0)                as role_vacancies_with_stack
    from market
    group by week, role_type

),

skills as (

    -- Разворачиваем stack: вакансия с пятью навыками даёт пять строк.
    -- unnest пустого или null массива даёт ноль строк — такие вакансии
    -- выше учтены только в знаменателе.
    select
        market.week,
        market.role_type,
        market.vacancy_key,
        market.is_spain,
        market.is_remote,
        {{ normalize_skill('skill_raw') }}              as skill
    from market
    cross join unnest(market.stack) as skill_raw
    where trim(skill_raw) != ''

)

select
    -- Ключ строки для тестов unique и not_null: грейн из трёх колонок, а
    -- тест unique в dbt без пакетов проверяет одну.
    concat(cast(skills.week as string), '|', skills.skill, '|', skills.role_type)
                                                        as skill_demand_key,
    skills.week,
    skills.skill,
    skills.role_type,
    -- count(distinct), а не count(*): после склейки написаний «Airflow» и
    -- «Apache Airflow» из одного stack становятся двумя строками одного
    -- навыка, а вакансия должна посчитаться один раз.
    count(distinct skills.vacancy_key)                  as vacancies_count,
    count(distinct if(skills.is_spain, skills.vacancy_key, null))
                                                        as spain_vacancies_count,
    count(distinct if(skills.is_remote, skills.vacancy_key, null))
                                                        as remote_vacancies_count,
    -- any_value: внутри группы неделя × роль знаменатель один и тот же.
    any_value(role_week.role_vacancies_count)           as role_vacancies_count,
    any_value(role_week.role_vacancies_with_stack)      as role_vacancies_with_stack

from skills
join role_week using (week, role_type)
group by skills.week, skills.skill, skills.role_type
