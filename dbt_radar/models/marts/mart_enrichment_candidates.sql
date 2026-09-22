-- Кандидаты на обогащение моделью и на догрузку страниц Adzuna.
--
-- Своих правил здесь нет намеренно: кто кандидат, решает
-- mart_vacancies_scored (блоки enrichment_rule и enrichment_ready). Эта
-- модель — только окно в витрину с нужными скриптам колонками. Так правило
-- живёт в одном месте, а скрипты перестают собирать условие у себя.
--
-- view, а не table: считать тут нечего, а view всегда показывает то, что
-- сейчас лежит в mart_vacancies_scored, — отдельно пересобирать его между
-- шагами пайплайна не нужно.

{{ config(materialized='view') }}

select
    vacancy_key,
    source,
    source_id,
    posted_at,
    title,
    company_name,
    location,
    job_types,
    salary_min,
    salary_max,
    salary_text,
    url,
    -- Текст для модели и откуда он: 'page' или 'api'.
    description_best,
    description_source,
    -- Готова ли вакансия к модели прямо сейчас (есть полный текст).
    -- Строки Adzuna без страницы тоже здесь: их ждёт fetch/adzuna_pages.py.
    is_ready_for_enrichment,
    -- Порядок очереди в обоих скриптах: сначала самые релевантные.
    relevance_score

from {{ ref('mart_vacancies_scored') }}

where is_enrichment_candidate
