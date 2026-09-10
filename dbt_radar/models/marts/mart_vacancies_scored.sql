-- Витрина с оценкой: ВСЕ вакансии за последние 14 дней, к каждой приписаны
-- признаки (роль, уровень, язык описания, география, агентство ли,
-- подходит ли локация по фактам от языковой модели), причина исключения
-- и балл релевантности.
--
-- Зачем отдельная модель, а не фильтр прямо в mart_vacancies_for_me:
-- здесь ничего не выбрасывается. Если вакансия не дошла до бота, в этой
-- таблице видно, КАКОЕ правило её отсеяло, и это можно сверить с ручной
-- разметкой. Фильтр в where не оставил бы следа — отлаживать вслепую.
--
-- ГРЕЙН: одна вакансия (vacancy_key), как в stg_vacancies. Join один —
-- left join к stg_enrichment, где на вакансию не больше одной строки,
-- поэтому строк не становится больше. group by нет; оконная функция
-- в блоке ranked только нумерует строки, но не схлопывает их.
--
-- select * ниже встречается только в промежуточных блоках, где он просто
-- протаскивает колонки дальше. Итоговый select перечисляет колонки явно —
-- именно он определяет, что попадёт в таблицу.

{{ config(materialized='table') }}

with enrichment as (

    -- Факты от языковой модели. Берём только то, что нужно правилу
    -- fits_location, и добавляем префикс llm_: у нас уже есть своя
    -- seniority по заголовку, и без префикса колонки модели путались бы
    -- с нашими признаками.
    select
        vacancy_key,
        work_mode                                   as llm_work_mode,
        location_city                               as llm_location_city,
        residency_requirement                       as llm_residency_requirement,
        enriched_at                                 as llm_enriched_at
    from {{ ref('stg_enrichment') }}

),

vacancies as (

    select
        *,

        -- Заготовка для признака is_english: доля английских служебных слов
        -- среди всех слов в первых 2000 символах описания. Служебные слова
        -- (the, and, for...) есть в любом английском тексте, о чём бы он ни был.
        --
        -- Доля, а не просто число: у Adzuna описание — обрывок в ~75 слов,
        -- у arbeitnow — текст в ~215 слов. Порог «не меньше N маркеров»
        -- был бы для одного источника слишком строгим, для другого слишком
        -- мягким. Доля от длины текста не зависит.
        --
        -- regexp_extract_all возвращает массив совпадений, array_length —
        -- их число. \S+ — подряд идущие непробельные символы, то есть слово.
        -- safe_divide вместо «/»: при нуле слов вернёт null вместо ошибки
        -- деления на ноль; coalesce превращает этот null в 0.
        coalesce(safe_divide(
            array_length(regexp_extract_all(
                lower(substr(coalesce(description, ''), 1, 2000)),
                r'\b(?:the|and|for|with|you|are|our|this|will|we)\b'
            )),
            array_length(regexp_extract_all(
                substr(coalesce(description, ''), 1, 2000),
                r'\S+'
            ))
        ), 0)                                           as english_marker_share,

        -- Ядро заголовка для дедупа — всё до первого разделителя:
        -- «Data Engineer - Payments (m/w/d)» → «data engineer».
        -- Шаги изнутри наружу:
        --   1. убрать скобку в самом начале. Иначе «(Senior) CRM Manager»
        --      обрезался бы по этой скобке до пустой строки (146 таких
        --      заголовков), и все они у одной компании склеились бы в дубль;
        --   2. отрезать всё от первого разделителя до конца строки.
        --      Разделители: дефис с пробелом рядом, –, —, |, запятая, «(».
        --      Дефис ВНУТРИ слова разделителем не считаем: иначе
        --      «AI-Native Data Engineer» превратился бы в «ai»;
        --   3. схлопнуть пробелы в один и обрезать края.
        -- title_normalized из staging уже в нижнем регистре.
        trim(regexp_replace(
            regexp_replace(
                regexp_replace(title_normalized, r'^\s*\([^)]*\)', ''),
                r'(\s-|-\s|[–—|,(]).*$', ''
            ),
            r'\s+', ' '
        ))                                              as title_core

    from {{ ref('stg_vacancies') }}

    -- left join, а не join: обогащения может не быть — вакансию ещё не
    -- успели обогатить. Обычный join выбросил бы такие строки, а эта модель
    -- не выбрасывает ничего. using (vacancy_key) — то же, что
    -- on a.vacancy_key = b.vacancy_key, но колонка в результате одна.
    left join enrichment using (vacancy_key)

    -- Окно в две недели. Строки без posted_at сравнение отсекает (null >= x
    -- даёт null, а не true) — вакансию без даты считать свежей нельзя.
    where posted_at >= timestamp_sub(current_timestamp(), interval 14 day)

),

features as (

    select
        *,

        -- Порядок веток важен: case берёт ПЕРВУЮ сработавшую. Поэтому
        -- «Business Intelligence Analyst» станет analytics_engineer, а не analyst.
        --
        -- product_analyst обязан стоять ДО analyst: в «product data analyst»
        -- содержится «data analyst», и ветка analyst забрала бы его первой.
        -- Из списка analyst «product analyst» убран — теперь у него своя ветка.
        case
            when regexp_contains(title_normalized,
                r'\b(analytics engineer|bi engineer|bi developer|business intelligence)\b')
                then 'analytics_engineer'
            when regexp_contains(title_normalized,
                r'\b(product analyst|product data analyst)\b')
                then 'product_analyst'
            when regexp_contains(title_normalized,
                r'\b(data analyst|bi analyst)\b')
                then 'analyst'
            when regexp_contains(title_normalized, r'\bdata engineer\b')
                then 'data_engineer'
            else 'other'
        end                                             as role_type,

        -- Тот же принцип: «Senior Lead Engineer» — это lead.
        -- \bsr\b ловит и «Sr.», точка — не буква, граница слова на месте.
        -- Слова ищем целиком, с \b с обеих сторон: иначе intern нашёлся бы
        -- в international и internal. Цена — не ловятся формы вроде
        -- «Praktikant» или «Werkstudentin»: их при нужде добавляем в список.
        case
            when regexp_contains(title_normalized,
                r'\b(lead|head|director|chief|vp|principal)\b')
                then 'lead'
            when regexp_contains(title_normalized, r'\b(senior|sr)\b')
                then 'senior'
            when regexp_contains(title_normalized,
                r'\b(junior|intern|internship|werkstudent|praktikum|trainee|graduate|becario)\b')
                then 'junior'
            else 'mid'
        end                                             as seniority,

        -- Английская ли вакансия. true, только если выполнены оба условия:
        --   1. английских маркеров не меньше 3 на 100 слов. Порог по данным:
        --      у английских текстов доля почти всегда выше 6%, у немецких,
        --      французских, испанских — обычно 0. Между 3% и 5% лежит всего
        --      несколько вакансий, и это в основном английские тексты, у
        --      которых начало описания занято HTML-разметкой. Поэтому 3%.
        --   2. в заголовке нет немецкого маркера пола (m/w/d), (m/f/d), (w/m/d).
        --      \s* допускает пробелы внутри: «(m/ w/ d)» тоже встречается.
        english_marker_share >= 0.03
            and not regexp_contains(title_normalized,
                r'\((?:m\s*/\s*w\s*/\s*d|m\s*/\s*f\s*/\s*d|w\s*/\s*m\s*/\s*d)\)')
                                                        as is_english,

        -- География. Ветка spain стоит первой: вакансия в Барселоне
        -- с пометкой remote получит 'spain' — это более сильный сигнал.
        -- coalesce: у Adzuna is_remote всегда null, location бывает пустой.
        case
            when regexp_contains(lower(coalesce(location, '')),
                r'barcelona|madrid|spain|españa')
                then 'spain'
            when coalesce(is_remote, false)
                 or regexp_contains(lower(coalesce(location, '')), r'remote')
                then 'remote'
            else 'other'
        end                                             as location_fit,

        -- Только \b в начале, без \b в конце: так найдутся «Randstad España»
        -- и «ManpowerGroup», но не «Whays». Точное сравнение имени целиком
        -- пропустило бы почти всех — агентства пишутся с хвостами.
        -- coalesce: у пустой компании regexp_contains вернёт null, а нам
        -- нужен честный false — «не агентство», а не «неизвестно».
        coalesce(regexp_contains(
            lower(company_name),
            r'\b(randstad|careerwise|hays|michael page|adecco|manpower|k-lagan|experis|page personnel)'
        ), false)                                       as is_agency

    from vacancies

),

fits_location_rule as (

    -- ПРАВИЛО «подходит ли мне локация» по фактам от языковой модели.
    -- Всё правило — один case ниже; остальная модель знает только
    -- результат fits_location. Меняем правило — меняем только этот блок.
    --
    -- Ветки проверяются сверху вниз, срабатывает первая:
    --   1. обогащения нет              → null: балл считается как раньше
    --   2. город Барселона             → yes, в любом режиме работы
    --   3. требуют жить не в Испании   → no
    --   4. удалёнка                    → yes
    --   5. офис, и город известен      → no (раз дошли сюда, не Барселона)
    --   6. всё остальное               → unclear
    -- Ветка 3 стоит перед 4 намеренно: так «remote без требования жить вне
    -- Испании» получается само, и условие про резидентство пишется один раз.
    --
    -- «Не в Испании» определяем грубо: требование есть, и в нём нет ни
    -- одного слова, под которое подходит житель Барселоны. «Must be based
    -- in the UK» → no; «right to work in the EU» → не no.
    select
        *,
        case
            when llm_enriched_at is null
                then null
            when regexp_contains(lower(coalesce(llm_location_city, '')), r'barcelona')
                then 'yes'
            when llm_residency_requirement is not null
                 and not regexp_contains(
                     lower(llm_residency_requirement),
                     r'spain|españa|espana|spanish|barcelona|madrid|\beu\b|european union|europe|emea'
                 )
                then 'no'
            when llm_work_mode = 'remote'
                then 'yes'
            when llm_work_mode = 'onsite' and llm_location_city is not null
                then 'no'
            else 'unclear'
        end                                             as fits_location

    from features

),

ranked as (

    select
        *,

        -- Дедуп по смыслу: одна и та же вакансия, выложенная несколько раз,
        -- в нескольких источниках или в нескольких городах.
        -- partition by — «группа»: одна компания + одно ядро заголовка.
        -- Локации в ключе нет намеренно: Zynga выкладывает одну и ту же
        -- «Senior Analytics Engineer» и в Барселоне, и в Мадриде.
        -- row_number нумерует строки внутри группы в порядке order by;
        -- номер 1 остаётся, остальные станут duplicate.
        --
        -- Первым ключом сортировки стоит «уже исключена предыдущим правилом».
        -- false сортируется раньше true, поэтому живые кандидаты идут первыми.
        -- Без этого свежая «Data Engineer (m/w/d)» получила бы номер 1 (и ушла
        -- как not_english), а более старая английская «Data Engineer» —
        -- номер 2 (и ушла как duplicate). Потеряли бы обе.
        -- vacancy_key в конце — чтобы при одинаковом posted_at результат
        -- не менялся от прогона к прогону.
        row_number() over (
            partition by lower(company_name), title_core
            order by
                (seniority in ('lead', 'junior') or not is_english),
                posted_at desc,
                vacancy_key
        )                                               as dedup_rank

    from fits_location_rule

)

select
    vacancy_key,
    source,
    source_id,
    posted_at,
    ingested_at,
    title,
    title_normalized,
    title_core,
    company_name,
    location,
    is_remote,
    salary_min,
    salary_max,
    salary_text,
    url,
    tags,
    job_types,
    description,
    -- Чистое описание из staging. Его читает скрипт обогащения.
    description_clean,

    role_type,
    seniority,
    is_english,
    location_fit,
    is_agency,

    llm_work_mode,
    llm_location_city,
    llm_residency_requirement,
    fits_location,

    -- Первое сработавшее правило. Если не сработало ни одно, case без else
    -- вернёт null — это и значит «вакансия проходит в подборку».
    case
        when seniority in ('lead', 'junior')    then 'wrong_seniority'
        when not is_english                     then 'not_english'
        when dedup_rank > 1                     then 'duplicate'
        when role_type = 'other'                then 'irrelevant_role'
    end                                                 as excluded_reason,

    -- Балл считаем для всех строк, в том числе исключённых: так при сверке
    -- с разметкой видно, высоко ли стояла бы отсеянная вакансия.
    (
        case role_type
            when 'analytics_engineer' then 4
            when 'analyst'            then 3
            when 'product_analyst'    then 2
            when 'data_engineer'      then 1
            else 0
        end
        -- География целиком через location_fit: и Испания, и удалёнка.
        + case location_fit
            when 'spain'  then 2
            when 'remote' then 1
            else 0
          end
        -- Локация по фактам от модели. Обогащения нет → fits_location null →
        -- ветка else → 0, и балл ровно такой же, как до появления модели.
        + case fits_location
            when 'yes' then 3
            when 'no'  then -5
            else 0
          end
        - case when seniority = 'senior' then 2 else 0 end
        -- is_agency в балл не входит: вакансии от агентств нас устраивают.
        -- Признак оставлен в таблице для анализа и сверки с разметкой.
        + case when salary_min is not null then 1 else 0 end
    )                                                   as relevance_score

from ranked
