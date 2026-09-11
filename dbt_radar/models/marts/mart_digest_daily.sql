-- Витрина дайджеста: вакансии, которые попадают в ежедневную подборку.
--
-- ГРЕЙН: одна строка = одна вакансия, отобранная в дайджест. Строк не
-- больше 10. Оба join — left join к моделям, где на вакансию не больше
-- одной строки (тесты unique в _staging.yml), поэтому строк не прибавляется.
--
-- Откуда что берётся:
--   mart_vacancies_scored — отбор (excluded_reason, posted_at), балл,
--     заголовок, компания, ссылка, источник текста;
--   stg_vacancy_pages     — признак снятой вакансии is_dead;
--   stg_enrichment        — факты от языковой модели: город, режим работы,
--     уровень, стек, зарплата, резидентство, суть.
--
-- Колонки идут в порядке, удобном для чтения человеком. vacancy_key —
-- последней: это служебный ключ, на нём стоят тесты.
--
-- Порядок строк в таблице при чтении не гарантирован: order by ниже нужен,
-- чтобы limit отобрал правильные 10. Потребителю — боту — нужен свой
-- order by по тем же ключам; «обогащена» там — prompt_version is not null.

{{ config(materialized='table') }}

with vacancies as (

    -- Отбор из витрины с оценкой: прошла все правила и свежая — не старше
    -- 7 дней на момент сборки. Строки без posted_at сравнение отсекает
    -- (null >= x даёт null, а не true).
    select
        vacancy_key,
        posted_at,
        title,
        company_name,
        url,
        relevance_score,
        description_source
    from {{ ref('mart_vacancies_scored') }}
    where excluded_reason is null
      and posted_at >= timestamp_sub(current_timestamp(), interval 7 day)

),

pages as (

    select
        vacancy_key,
        is_dead
    from {{ ref('stg_vacancy_pages') }}

),

enrichment as (

    -- Поля модели берём из stg_enrichment как есть: seniority здесь — оценка
    -- модели (по заголовку и требуемому опыту), а не наша по заголовку из
    -- mart_vacancies_scored; зарплата — найденная моделью в тексте.
    select
        vacancy_key,
        location_city,
        location_country,
        work_mode,
        seniority,
        stack,
        salary_min,
        salary_max,
        salary_currency,
        salary_period,
        residency_requirement,
        summary,
        prompt_version
    from {{ ref('stg_enrichment') }}

)

select
    vacancies.posted_at,
    vacancies.title,
    vacancies.company_name,
    enrichment.location_city,
    enrichment.location_country,
    enrichment.work_mode,
    enrichment.seniority,
    enrichment.stack,
    enrichment.salary_min,
    enrichment.salary_max,
    enrichment.salary_currency,
    enrichment.salary_period,
    enrichment.residency_requirement,
    enrichment.summary,
    vacancies.url,
    vacancies.relevance_score,
    vacancies.description_source,
    enrichment.prompt_version,
    vacancies.vacancy_key

from vacancies

-- left join: страница скачана не у каждой вакансии, обогащение есть не у
-- каждой. Обычный join выбросил бы такие вакансии, а их надо показать —
-- с пустыми полями.
left join pages
    on pages.vacancy_key = vacancies.vacancy_key
left join enrichment
    on enrichment.vacancy_key = vacancies.vacancy_key

-- Мёртвую вакансию уже исключает правило dead в mart_vacancies_scored,
-- но условие пишем явно: дайджест не должен зависеть от того, в каком
-- порядке там стоят правила. coalesce: страницы нет — is_dead null,
-- а not null дал бы null и выбросил вакансию.
where not coalesce(pages.is_dead, false)

order by
    vacancies.relevance_score desc,
    -- При равном балле обогащённые выше: у них в дайджесте есть стек,
    -- режим работы и суть. true при сортировке по убыванию идёт первым.
    enrichment.vacancy_key is not null desc,
    vacancies.posted_at desc,
    -- Последний ключ — чтобы при полном равенстве в первые 10 попадали
    -- одни и те же вакансии от сборки к сборке.
    vacancies.vacancy_key

limit 10
