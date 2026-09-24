-- Факты для дашборда: зарплатная вилка каждой вакансии.
--
-- ГРЕЙН: одна вакансия с указанной вилкой. Никаких агрегатов.
--
-- Зачем, если есть mart_salary. Счётчики складываются, медианы — нет:
-- медиану роли нельзя собрать из медиан ячеек роль × город. Looker Studio
-- при этом посчитает «медиану медиан» молча и выдаст правдоподобное
-- неверное число. Поэтому дашборду — факты на мелком зерне, а медиану на
-- любом уровне группировки он считает сам. mart_salary остаётся для
-- быстрого чтения глазами.
--
-- НАСЕЛЕНИЕ — рынок (is_market_vacancy), как у mart_salary и
-- mart_skill_demand, и только вакансии хотя бы с одной границей вилки.
-- ВИЛКА — salary_*_best: поле источника главнее ответа модели.
-- Периоды к одной шкале не приводятся — см. mart_salary.
-- ОГРАНИЧЕНИЕ — окно mart_vacancies_scored 30 дней: срез, а не история.

select
    s.vacancy_key,
    s.role_type,
    s.seniority,
    s.location_city                                     as city,
    -- Страна — только из ответа модели: своего поля страны у источников
    -- нет. У необогащённой вакансии null.
    e.location_country                                  as country,
    s.work_mode,
    s.source,
    -- Неделя — по тому же правилу, что в mart_skill_demand: isoweek от
    -- даты публикации, у Manfred — от первого появления в списке (его
    -- posted_at — это updatedAt). Правило записано в двух местах; меняешь
    -- здесь — поменяй и там.
    date_trunc(date(coalesce(m.first_seen_at, s.posted_at)), isoweek) as week,
    s.salary_min_best                                   as salary_from,
    s.salary_max_best                                   as salary_to,
    s.salary_period_best                                as salary_period,
    s.salary_currency_best                              as salary_currency,
    s.url

from {{ ref('mart_vacancies_scored') }} as s
-- left join: у вакансий не из Manfred нет строки в stg_manfred_facts, у
-- необогащённых — в stg_enrichment. Обычный join выбросил бы их.
left join {{ ref('stg_manfred_facts') }} as m using (vacancy_key)
left join {{ ref('stg_enrichment') }} as e using (vacancy_key)
where s.is_market_vacancy
  and (s.salary_min_best is not null or s.salary_max_best is not null)
