-- Слой staging для ответов языковой модели.
--
-- В raw.llm_enrichment на одну вакансию может оказаться несколько строк:
-- например, после смены промпта вакансии обогатят заново, или прогон
-- перезапустят вручную. Сырой слой хранит все ответы как есть, а здесь
-- по каждой вакансии остаётся самый свежий.
--
-- ГРЕЙН: одна вакансия (vacancy_key). Тест unique в _staging.yml его сторожит:
-- mart_vacancies_scored джойнит эту модель, и дубль здесь размножил бы
-- строки там.

with enrichment as (

    select
        vacancy_key,
        summary,
        responsibilities,
        requirements,
        stack,
        seniority,
        domain,
        work_mode,
        location_city,

        -- Модель просили ISO-код страны и валюты, но «es» и «ES» не должны
        -- оказаться разными странами. Регистр приводим здесь, в staging.
        upper(location_country)                     as location_country,
        residency_requirement,
        salary_min,
        salary_max,
        upper(salary_currency)                      as salary_currency,
        lower(salary_period)                        as salary_period,

        -- В raw дата лежит строкой, как её вернула модель. safe_cast, а не
        -- cast: если модель напишет «end of September», cast уронил бы всю
        -- модель, а safe_cast вернёт null только в этой строке.
        safe_cast(application_deadline as date)     as application_deadline,

        benefits,
        lower(language)                             as language,
        model_name,
        prompt_version,
        enriched_at

    from {{ source('raw', 'llm_enrichment') }}

)

select *
from enrichment

-- Та же техника, что в stg_vacancies: пронумеровать ответы внутри вакансии
-- от свежего к старому и оставить первый.
qualify row_number() over (
    partition by vacancy_key
    order by enriched_at desc
) = 1
