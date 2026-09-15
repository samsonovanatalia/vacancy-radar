-- Очередь на отправку в Telegram: все подходящие вакансии, у которых ещё нет
-- исхода отправки. Бот (notify/telegram.py) берёт из неё первые MAX_PER_RUN
-- по queue_position; остальные остаются здесь и уйдут в следующий раз.
--
-- Раньше витрина называлась mart_digest_daily и держала топ-10 дня: вакансия
-- не из десятки до меня не доходила, пока не устаревала. Теперь число строк
-- не ограничено, а лимит живёт в боте и только откладывает.
--
-- ГРЕЙН: одна строка = одна вакансия в очереди. Оба join — left join к
-- моделям, где на вакансию не больше одной строки (тесты unique в
-- _staging.yml), поэтому строк не прибавляется. not exists к stg_digest_sent
-- строки только убирает.
--
-- Откуда что берётся:
--   mart_vacancies_scored — отбор (excluded_reason, source, posted_at), балл,
--     заголовок, компания, ссылка, источник текста;
--   stg_vacancy_pages     — снята ли вакансия (is_dead) и когда её последний
--     раз успешно проверили (last_checked_at);
--   stg_enrichment        — факты от языковой модели: город, режим работы,
--     уровень, стек, зарплата, резидентство, суть;
--   stg_digest_sent       — у каких вакансий уже есть исход отправки:
--     доставлена или недоставляемая.
--
-- Порядок очереди — колонка queue_position, а не order by. Строки в таблице
-- хранятся без порядка: order by имел смысл, пока по нему limit отбирал
-- десятку, а без limit он ничего бы не дал. Номер в колонке виден любому,
-- кто читает витрину, и бот сортирует по нему, не повторяя у себя правила.
--
-- Колонки идут в порядке, удобном для чтения человеком. vacancy_key —
-- последней: это служебный ключ, на нём стоят тесты.

{{ config(materialized='table') }}

with vacancies as (

    -- Отбор из витрины с оценкой: прошла все правила. Возраст здесь не
    -- ограничиваем — живость проверяется ниже, в where.
    select
        vacancy_key,
        source,
        posted_at,
        title,
        company_name,
        url,
        relevance_score,
        description_source,

        -- На чём основано решение о живости. Решает источник, а не то, есть
        -- ли у вакансии строка в stg_vacancy_pages: у вакансии Adzuna, до
        -- которой проверка ещё не дошла, строки нет, но судить о ней по дате
        -- нельзя — проверка для Adzuna есть, а без неё вакансию не берём.
        -- Страницы сейчас скачиваются только для Adzuna (fetch/adzuna_pages.py).
        -- Появится проверка у другого источника — дописать его в список.
        case
            when source in ('adzuna') then 'checked'
            else 'assumed_by_date'
        end                                         as liveness_basis

    from {{ ref('mart_vacancies_scored') }}
    where excluded_reason is null

),

pages as (

    select
        vacancy_key,
        is_dead,
        last_checked_at
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

),

sent as (

    -- Любой исход — и sent, и undeliverable: доставленную вакансию второй раз
    -- не шлём, недоставляемую не пробуем снова.
    select
        vacancy_key
    from {{ ref('stg_digest_sent') }}

)

select
    -- Номер в очереди, 1 — отправить первой. row_number нумерует 1, 2, 3...
    -- без повторов: последний ключ сортировки, vacancy_key, у двух строк
    -- совпасть не может. Окно считается после where, поэтому нумеруются
    -- только строки, попавшие в очередь, и номера идут без пропусков.
    row_number() over (
        order by
            vacancies.relevance_score desc,
            -- При равном балле обогащённые выше: у них в сообщении есть стек,
            -- формат работы и суть. true при сортировке по убыванию идёт первым.
            enrichment.vacancy_key is not null desc,
            vacancies.posted_at desc,
            -- Последний ключ — чтобы при полном равенстве номера не менялись
            -- от сборки к сборке.
            vacancies.vacancy_key
    )                                               as queue_position,
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
    vacancies.liveness_basis,
    enrichment.prompt_version,
    vacancies.vacancy_key

from vacancies

-- left join: страница скачана не у каждой вакансии, обогащение есть не у
-- каждой. Обычный join выбросил бы такие вакансии, а их надо показать —
-- с пустыми полями. Какие из них не брать, решает where.
left join pages
    on pages.vacancy_key = vacancies.vacancy_key
left join enrichment
    on enrichment.vacancy_key = vacancies.vacancy_key

-- Мёртвую вакансию уже исключает правило dead в mart_vacancies_scored,
-- но условие пишем явно: очередь не должна зависеть от того, в каком
-- порядке там стоят правила. coalesce: страницы нет — is_dead null,
-- а not null дал бы null и выбросил вакансию.
where not coalesce(pages.is_dead, false)

  -- Живость. Ветка выбирается по liveness_basis:
  --   checked         — последняя успешная проверка не старше 3 дней.
  --     Страницу перепроверяют, когда проверке больше суток
  --     (RECHECK_AFTER_DAYS в fetch/adzuna_pages.py), а прогон идёт раз в
  --     сутки. Если до суток не хватило нескольких минут, перепроверка
  --     сдвинется на следующий прогон, и проверке будет почти двое суток.
  --     Порог в 3 дня это покрывает и оставляет запас на один сорвавшийся
  --     прогон.
  --     Не проверялась — last_checked_at null, сравнение даёт null, и where
  --     строку отбрасывает. Это и нужно: непроверенную не берём.
  --   assumed_by_date — проверять нечем, заменитель — дата публикации:
  --     не старше 30 дней. mart_vacancies_scored сейчас и так берёт только
  --     30 дней, но условие пишем явно — по той же причине, что is_dead выше.
  --
  -- Оговорка: окно в 30 дней в mart_vacancies_scored действует и на Adzuna.
  -- Проверенная живая вакансия старше 30 дней сюда не дойдёт.
  and case vacancies.liveness_basis
        when 'checked'
            then pages.last_checked_at >= timestamp_sub(current_timestamp(), interval 3 day)
        when 'assumed_by_date'
            then vacancies.posted_at >= timestamp_sub(current_timestamp(), interval 30 day)
      end

  -- Вакансию с исходом отправки в очередь не ставим. not exists, а не
  -- not in (select ...): окажись в подзапросе хоть один null, not in не
  -- вернул бы ни одной строки.
  and not exists (
      select 1
      from sent
      where sent.vacancy_key = vacancies.vacancy_key
  )
