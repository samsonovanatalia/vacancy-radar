-- Витрина с оценкой: ВСЕ вакансии за последние 30 дней, к каждой приписаны
-- признаки (роль, уровень, язык описания, география, агентство ли,
-- подходит ли локация по фактам от языковой модели), причина исключения
-- и балл релевантности.
--
-- Зачем отдельная модель, а не фильтр прямо в mart_vacancies_for_me:
-- здесь ничего не выбрасывается. Если вакансия не дошла до бота, в этой
-- таблице видно, КАКОЕ правило её отсеяло, и это можно сверить с ручной
-- разметкой. Фильтр в where не оставил бы следа — отлаживать вслепую.
--
-- ГРЕЙН: одна вакансия (vacancy_key), как в stg_vacancies. Join два —
-- left join к stg_enrichment и к stg_vacancy_pages, в обеих на вакансию
-- не больше одной строки, поэтому строк не становится больше. group by нет; оконная функция
-- в блоке ranked только нумерует строки, но не схлопывает их.
--
-- select * ниже встречается только в промежуточных блоках, где он просто
-- протаскивает колонки дальше. Итоговый select перечисляет колонки явно —
-- именно он определяет, что попадёт в таблицу.

{{ config(materialized='table') }}

with enrichment as (

    -- Факты от языковой модели. Берём только то, что нужно правилам
    -- fits_location, foreign_language_required и residency_too_long, и
    -- добавляем префикс llm_: у нас уже есть своя seniority по заголовку, и
    -- без префикса колонки модели путались бы с нашими признаками.
    select
        vacancy_key,
        work_mode                                   as llm_work_mode,
        location_city                               as llm_location_city,
        residency_requirement                       as llm_residency_requirement,
        required_languages                          as llm_required_languages,
        residency_years_required                    as llm_residency_years_required,

        -- Зарплата, найденная моделью в тексте. Правил по ней нет — она
        -- нужна боту; витрина сводит её с зарплатой источника в блоке facts.
        salary_min                                  as llm_salary_min,
        salary_max                                  as llm_salary_max,
        salary_currency                             as llm_salary_currency,
        salary_period                               as llm_salary_period,

        enriched_at                                 as llm_enriched_at,

        -- Проверен ли язык: есть ли ответ промпта v5 или новее — с v5 модель
        -- возвращает required_languages. Именно «v5 или новее», а не «текущей
        -- версии»: иначе каждое поднятие версии промпта разом отправляло бы
        -- все неанглийские вакансии на удержание, пока их не обогатят заново.
        --
        -- Номер сравниваем числом, а не строкой: строкой 'v10' < 'v5'.
        -- stg_enrichment оставляет самый свежий ответ, а версии со временем
        -- только растут, так что «свежий ответ ≥ v5» = «есть ответ ≥ v5».
        coalesce(safe_cast(substr(prompt_version, 2) as int64) >= 5, false)
                                                    as llm_has_language_check
    from {{ ref('stg_enrichment') }}

),

manfred_facts as (

    -- Структурные поля Manfred: требуемые языки, процент удалёнки, города.
    -- Префикс src_ — по той же причине, что llm_ у модели: сразу видно,
    -- откуда поле. Сводятся с ответом модели в блоке facts.
    select
        vacancy_key,
        required_languages                          as src_required_languages,
        remote_percentage                           as src_remote_percentage,
        location_cities                             as src_location_cities,
        salary_currency                             as src_salary_currency,
        salary_period                               as src_salary_period,
        last_seen_at                                as src_last_seen_at,
        true                                        as has_source_facts
    from {{ ref('stg_manfred_facts') }}

),

pages as (

    -- Признак снятой вакансии со страницы. Страницы скачиваются пока только
    -- для Adzuna: у вакансий других источников строки здесь нет, и после
    -- left join is_dead у них null — правило dead их не трогает.
    select
        vacancy_key,
        is_dead
    from {{ ref('stg_vacancy_pages') }}

),

vacancies as (

    select
        *,

        -- Заготовка для признака is_english: доля английских служебных слов
        -- среди всех слов в первых 2000 символах чистого описания. Служебные
        -- слова (the, and, for...) есть в любом английском тексте, о чём бы
        -- он ни был.
        --
        -- Считаем по description_clean, а не по сырому description. У arbeitnow
        -- первые 2000 сырых символов наполовину занимала HTML-разметка: в окно
        -- попадало меньше текста, а атрибуты вроде style="font-family:Lato,"
        -- считались словами. Доля искажалась в обе стороны.
        --
        -- Доля, а не просто число: у Adzuna описание — обрывок в ~75 слов,
        -- у arbeitnow в окно попадает ~280 слов. Порог «не меньше N маркеров»
        -- был бы для одного источника слишком строгим, для другого слишком
        -- мягким. Доля от длины текста не зависит.
        --
        -- regexp_extract_all возвращает массив совпадений, array_length —
        -- их число. \S+ — подряд идущие непробельные символы, то есть слово.
        -- safe_divide вместо «/»: при нуле слов вернёт null вместо ошибки
        -- деления на ноль; coalesce превращает этот null в 0.
        coalesce(safe_divide(
            array_length(regexp_extract_all(
                lower(substr(coalesce(description_clean, ''), 1, 2000)),
                r'\b(?:the|and|for|with|you|are|our|this|will|we)\b'
            )),
            array_length(regexp_extract_all(
                substr(coalesce(description_clean, ''), 1, 2000),
                r'\S+'
            ))
        ), 0)                                           as english_marker_share,

        -- Название компании к общему виду — вторая половина ключа дедупа.
        -- Правило вынесено в макрос: «что такое одна и та же компания»
        -- должно жить в одном месте. Подробности и что он НЕ делает —
        -- в macros/normalize_company_name.sql.
        {{ normalize_company_name('company_name') }}    as company_core,

        -- Ключ заголовка для дедупа — заголовок ЦЕЛИКОМ, из которого
        -- вычищен только шум: маркеры пола, города и страны, ставки,
        -- лишняя пунктуация. Правило вынесено в макрос, подробности и
        -- размен, на который мы идём, — в macros/normalize_job_title.sql.
        --
        -- До 2026-09-20 здесь была обрезка до первого разделителя. Она
        -- резала вместе с шумом и смысл: у N26 «Data Analyst - Finance»,
        -- «- Investments» и «- Ops Automation» схлопывались в одну строку,
        -- и в подборку попадала одна вакансия из четырёх.
        --
        -- title_normalized из staging уже в нижнем регистре.
        {{ normalize_job_title('title_normalized') }}   as title_key

    from {{ ref('stg_vacancies') }}

    -- left join, а не join: обогащения может не быть — вакансию ещё не
    -- успели обогатить. Обычный join выбросил бы такие строки, а эта модель
    -- не выбрасывает ничего. using (vacancy_key) — то же, что
    -- on a.vacancy_key = b.vacancy_key, но колонка в результате одна.
    left join enrichment using (vacancy_key)

    -- То же для страниц: страницы нет — вакансия остаётся, is_dead null.
    left join pages using (vacancy_key)

    -- И для структурных полей: они есть только у Manfred.
    left join manfred_facts using (vacancy_key)

    -- Окно свежести. У большинства источников это 30 дней от публикации:
    -- столько вакансия может ждать в очереди на отправку (mart_digest_queue).
    -- Строки без posted_at сравнение отсекает (null >= x даёт null, а не
    -- true) — вакансию без даты считать свежей нельзя.
    --
    -- У Manfred даты публикации нет вовсе: коллектор кладёт в неё updatedAt,
    -- а он бывает четырёхлетней давности у открытой вакансии. Зато список
    -- офферов отдаёт ТОЛЬКО активные, поэтому живость там — «вакансию видели
    -- в списке» (src_last_seen_at из stg_manfred_facts).
    --
    -- Допуск двое суток, а не сутки: сбор идёт раз в день, и один упавший
    -- прогон не должен выносить из подборки весь источник. Обратная сторона
    -- — закрытая вакансия живёт в подборке ещё день. По updatedAt она жила бы
    -- до месяца, а половина офферов не попадала бы в подборку вовсе.
    where case
              when source = 'manfred'
                  then src_last_seen_at >= timestamp_sub(current_timestamp(), interval 2 day)
              else posted_at >= timestamp_sub(current_timestamp(), interval 30 day)
          end

),

facts as (

    -- ФАКТЫ О ВАКАНСИИ: ПОЛЕ ИСТОЧНИКА ГЛАВНЕЕ МОДЕЛИ. Единственное место
    -- в витрине, где поле источника сводится с ответом модели; все правила
    -- ниже читают только эти колонки.
    --
    -- Почему источник главнее: структурное поле компания заполнила сама,
    -- выбрав из списка, — ошибиться в нём почти нечем. Ответ модели — это
    -- чтение свободного текста, и он ошибается: 22.09.2026 v6 назвала
    -- hybrid вакансию с «Choose to work 100% remotely». Поэтому модель —
    -- только там, где поля нет. Пока такие поля есть только у Manfred
    -- (stg_manfred_facts); у остальных источников src_* пустые, и всё
    -- решает модель, как раньше.
    --
    -- Все четыре колонки — без префикса: это уже не «ответ модели» и не
    -- «поле источника», а итог. Откуда он взят, видно по has_source_facts
    -- и по llm_* рядом в итоговой таблице.
    select
        *,

        -- Формат работы из remote_percentage: 100 — полностью удалённая,
        -- 0 — офис, между — гибрид. Процента нет — модель.
        coalesce(
            case
                when src_remote_percentage = 100 then 'remote'
                when src_remote_percentage = 0   then 'onsite'
                when src_remote_percentage > 0   then 'hybrid'
            end,
            llm_work_mode
        )                                               as work_mode,

        -- Город — первый из списка источника. Список пуст (полная
        -- удалёнка) — safe_offset даёт null, и слово за моделью.
        -- Барселону, стоящую в списке не первой («Madrid, España,
        -- Barcelona, España»), не теряем: is_barcelona ищет её во всём
        -- поле location.
        coalesce(
            src_location_cities[safe_offset(0)],
            llm_location_city
        )                                               as location_city,

        -- Требуемые языки. Не coalesce, а проверка на пустоту: пустой
        -- languages у Manfred значит «компания не указала», а не «язык не
        -- нужен», — массив при этом не null, а пустой, и coalesce его
        -- принял бы. Такие вакансии решает модель, как у всех источников.
        case
            when array_length(src_required_languages) > 0
                then src_required_languages
            else llm_required_languages
        end                                             as required_languages,

        -- Проверен ли язык: источник назвал языки или есть ответ модели
        -- v5+. Если нет ни того, ни другого, вакансию на другом языке
        -- придерживает language_rule — в том числе вакансию Manfred с
        -- пустым languages, пока модель недоступна. Это не обходим.
        coalesce(array_length(src_required_languages) > 0, false)
            or coalesce(llm_has_language_check, false)  as has_language_check,

        -- Зарплата: тот же принцип, что выше, — источник главнее модели.
        -- Числа salary_min и salary_max приходят из stg_vacancies, то есть
        -- прямо из полей источника (Manfred, Adzuna, remoteok). Модель
        -- вычитывает зарплату из текста и иногда ошибается; поле источника
        -- компания заполнила сама.
        --
        -- Вилку берём ЦЕЛИКОМ с одной стороны, а не по числу: смешать
        -- нижнюю границу источника с верхней от модели значило бы показать
        -- вилку, которой нет ни в одном источнике.
        case when salary_min is not null or salary_max is not null
            then salary_min else llm_salary_min
        end                                             as salary_min_best,
        case when salary_min is not null or salary_max is not null
            then salary_max else llm_salary_max
        end                                             as salary_max_best,

        -- Валюта и период — coalesce, а не «вместе с вилкой»: у источников,
        -- кроме Manfred, чисел два, а валюты и периода нет вовсе. Без
        -- запасного варианта бот печатал бы «45 000–55 000» без валюты.
        -- Размен: если модель прочитала валюту неверно, ошибка ляжет рядом
        -- с верной суммой источника. Обе величины при этом из одного
        -- объявления, так что разойтись им особо негде.
        coalesce(src_salary_currency, llm_salary_currency)
                                                        as salary_currency_best,
        coalesce(src_salary_period, llm_salary_period)  as salary_period_best

    from vacancies

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
        --   1. английских маркеров не меньше 5 на 100 слов чистого текста.
        --      Порог по данным на 10.09.2026: у английских текстов доля почти
        --      всегда выше 8%, у немецких, французских, испанских — почти
        --      всегда ниже 2%. Между ними лежат в основном смешанные тексты:
        --      английское «About us», а дальше описание на немецком или
        --      французском. В полосе 4–5% таких 17 и одна английская
        --      вакансия, поэтому 5%, а не 3%. Выше 5% смешанные тексты ещё
        --      встречаются, но там же лежат английские, и одним порогом
        --      их уже не разделить.
        --   2. в заголовке нет немецкого маркера пола (m/w/d), (m/f/d), (w/m/d).
        --      \s* допускает пробелы внутри: «(m/ w/ d)» тоже встречается.
        english_marker_share >= 0.05
            and not regexp_contains(title_normalized,
                r'\((?:m\s*/\s*w\s*/\s*d|m\s*/\s*f\s*/\s*d|w\s*/\s*m\s*/\s*d)\)')
                                                        as is_english,

        -- Русская ли вакансия: не меньше половины слов в начале текста
        -- содержат кириллицу. Служебные слова, как у is_english, здесь не
        -- нужны: кириллицу в тексте на другом языке почти не встретишь, и
        -- одна эта примета отделяет русский текст надёжно. Половина, а не
        -- «хоть одно слово» — чтобы английский текст с русским названием
        -- компании не стал русским.
        coalesce(safe_divide(
            array_length(regexp_extract_all(
                substr(coalesce(description_clean, ''), 1, 2000),
                r'\S*[а-яА-ЯёЁ]\S*'
            )),
            array_length(regexp_extract_all(
                substr(coalesce(description_clean, ''), 1, 2000),
                r'\S+'
            ))
        ), 0) >= 0.5                                    as is_russian,

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

        -- Барселона по ДВУМ полям сразу: итоговому городу location_city
        -- (блок facts: город из структурного поля источника, а где его нет —
        -- из обогащения) и полю location из источника.
        --
        -- Поля неравноценны, и работают они по-разному.
        -- Город из обогащения — это то же самое поле location, разобранное
        -- моделью до названия города (промпт так и велит: «take them from
        -- the Location known field first»), плюс город из текста описания,
        -- если в location его не было. То есть это location, причёсанный и
        -- дополненный. Поэтому «какой это город» спрашиваем у него.
        -- Само location — свободный текст шести разных API: там и
        -- «Barcelona, Cataluña», и «Spain», и «Worldwide», и пустая строка.
        --
        -- Отсюда асимметрия, ради которой поле и осталось в правиле: слову
        -- «barcelona» внутри location верим — ошибиться в эту сторону почти
        -- нечем. А его ОТСУТСТВИЮ не верим: «Spain» не значит, что вакансия
        -- не в Барселоне. Поэтому location подтверждает Барселону, но
        -- никогда её не опровергает; «город известен, и он не Барселона»
        -- решает только location_city.
        --
        -- regexp_contains, а не сравнение целиком: модель возвращает и
        -- «Barcelona», и «Barcelona (hybrid)», источник — «Barcelona,
        -- Cataluña, Spain». coalesce на '' — чтобы у пустого поля вышел
        -- честный false, а не null.
        regexp_contains(lower(coalesce(location_city, '')), r'barcelona')
            or regexp_contains(lower(coalesce(location, '')), r'barcelona')
                                                        as is_barcelona,

        -- Только \b в начале, без \b в конце: так найдутся «Randstad España»
        -- и «ManpowerGroup», но не «Whays». Точное сравнение имени целиком
        -- пропустило бы почти всех — агентства пишутся с хвостами.
        -- coalesce: у пустой компании regexp_contains вернёт null, а нам
        -- нужен честный false — «не агентство», а не «неизвестно».
        coalesce(regexp_contains(
            lower(company_name),
            r'\b(randstad|careerwise|hays|michael page|adecco|manpower|k-lagan|experis|page personnel)'
        ), false)                                       as is_agency,

        -- Заголовок продаж, работы с клиентами или найма — не роль про данные,
        -- даже если в заголовке есть слова про данные. «Account Executive,
        -- Business Intelligence» ветка analytics_engineer в role_type забирает
        -- по словам business intelligence, хотя это продажи.
        -- \b с обеих сторон — слова целиком: «Salesforce» и «presales» не ловятся.
        -- Цена: «Sales Data Analyst» тоже отсеется. На 11.09.2026 таких заголовков
        -- в подборке нет — «Sales Analyst» и похожие уже исключает irrelevant_role.
        regexp_contains(title_normalized,
            r'\b(account executive|sales|business development|account manager|customer success|recruiter)\b')
                                                        as is_non_data_title,

        -- Требуется язык, кроме английского и русского (поле промпта v5):
        -- на рабочем уровне у меня только эти два. Испанский базовый,
        -- французский средний — для требования в вакансии ни тот, ни другой
        -- не годятся, поэтому уровень не смотрим: «будет плюсом» модель в
        -- список не берёт, так что сам факт попадания языка в список уже
        -- значит требование. Английский и русский не проверяем: на
        -- 15.09.2026 английский есть в 22 списках из 23, отсекать им нечего.
        -- С 2026-09-22 это правило решает за язык и у вакансий, написанных
        -- не на английском (см. language_rule), — раньше их отсекал
        -- not_english по языку текста.
        -- lower: модель просили код ISO 639-1, но «EN» и «en» не должны
        -- разойтись. У языков из источника код тоже приведён к нижнему
        -- регистру (stg_manfred_facts). Языков нет ни у источника, ни у
        -- модели — required_languages null, unnest
        -- даёт ноль строк, и exists честно возвращает false, а не null.
        exists (
            select 1
            from unnest(required_languages) as l
            where lower(l.language) not in ('en', 'ru')
        )                                               as is_foreign_language_required,

        -- Требуют больше года проживания в стране (поле промпта v5).
        -- coalesce: требования нет или обогащения нет — сравнение даёт null,
        -- а нужен честный false. Иначе null в сортировке дедупа ниже встал бы
        -- раньше false и сдвинул бы, какая копия вакансии остаётся.
        coalesce(llm_residency_years_required > 1, false) as is_residency_too_long

    from facts

),

language_rule as (

    -- ЯЗЫКОВОЕ ПРАВИЛО с 2026-09-22. Язык объявления сам по себе больше
    -- не причина исключения: испанская вакансия в Барселоне может не
    -- требовать испанского. Решает то, ЧТО требуют (required_languages,
    -- правило foreign_language_required), а не на чём написано.
    --   - на английском или русском — проходит, как раньше;
    --   - на другом языке, язык уже проверен моделью (ответ v5+) —
    --     решает foreign_language_required;
    --   - на другом языке и не проверен — придерживаем
    --     (awaiting_language_check), пока модель не прочитает. Пропускать
    --     непроверенную нельзя: в дайджест утекла бы вакансия с
    --     обязательным испанским.
    -- Придержанная вакансия — кандидат на обогащение (язык к правилам без
    -- модели не относится), поэтому ждёт она обычно до следующего
    -- утреннего прогона.
    --
    -- Отдельный блок, потому что в одном select нельзя сослаться на
    -- колонки is_english и is_russian, которые в нём же и вычисляются.
    select
        *,
        not is_english
            and not is_russian
            and not has_language_check
                                                        as is_awaiting_language_check

    from features

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
            when llm_enriched_at is null and not coalesce(has_source_facts, false)
                then null
            when regexp_contains(lower(coalesce(location_city, '')), r'barcelona')
                then 'yes'
            when llm_residency_requirement is not null
                 and not regexp_contains(
                     lower(llm_residency_requirement),
                     r'spain|españa|espana|spanish|barcelona|madrid|\beu\b|european union|europe|emea'
                 )
                then 'no'
            when work_mode = 'remote'
                then 'yes'
            when work_mode = 'onsite' and location_city is not null
                then 'no'
            else 'unclear'
        end                                             as fits_location

    from language_rule

),

wrong_location_rule as (

    -- ПРАВИЛО «Барселона или полная удалёнка». Подходит либо работа в
    -- Барселоне — офис, гибрид, неважно, — либо полная удалёнка откуда
    -- угодно. Всё остальное, включая Мадрид с офисом или гибридом, мимо.
    --
    -- Зачем отдельное правило, а не fits_location выше. fits_location
    -- отвечает на другой вопрос — «насколько локация мне подходит» — и
    -- влияет только на балл (+3 / −5). В него подмешано требование к
    -- резидентству, а гибрид в чужом городе даёт у него 'unclear', то есть
    -- «не знаю». Здесь же нужен жёсткий да/нет по географии. Размен: два
    -- признака про локацию рядом; взамен каждый читается и меняется
    -- отдельно, не задевая второй.
    --
    -- Ветки сверху вниз, срабатывает первая:
    --   1. Барселона                        → подходит, в любом формате
    --   2. полная удалёнка                  → подходит, из любого города
    --   3. город известен и это не Барселона → не подходит
    --   4. города не знаем, но знаем формат, и он не remote → не подходит
    --   5. не знаем ни города, ни формата   → НЕ исключаем
    --
    -- Ветка 5 — сознательное решение, а не недосмотр. Исключить вакансию,
    -- которую мы просто не изучили, значит выбросить её за наше незнание,
    -- а не за несоответствие. Такая вакансия идёт дальше и проигрывает
    -- там, где и должна, — в баллах.
    --
    -- work_mode сравниваем со списком, а не с «is not null»: в промпте у
    -- него есть отдельное значение 'unclear' — «в тексте не сказано».
    -- Оно означает ровно «не знаю», и считать его известным форматом
    -- нельзя, иначе ветка 4 выбросит всё необогащённое пополам с тем,
    -- что модель честно не смогла прочитать.
    select
        *,
        case
            when is_barcelona                           then false
            when work_mode = 'remote'                   then false
            when location_city is not null              then true
            when work_mode in ('onsite', 'hybrid')      then true
            else false
        end                                             as is_wrong_location

    from fits_location_rule

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
        -- Первые ключи сортировки — «вакансию всё равно исключит другое
        -- правило»: сначала мёртвая ли она, потом исключённые грейд, язык и
        -- заголовок не про данные.
        -- Язык с 2026-09-22 сам не исключает (см. language_rule), но в
        -- сортировке остался как предпочтение: английская копия проходит
        -- сразу, а неанглийская ждёт проверки модели. Язык текста известен
        -- у всех копий, так что сортировать по нему можно — в отличие от
        -- ответов модели, см. следующий абзац.
        --
        -- Правил по ответам модели (foreign_language_required,
        -- residency_too_long) здесь нет намеренно. Модель обогащает только
        -- кандидатов (enrichment_rule), то есть победителя дедупа; у его копий ответа
        -- нет, и флаг у них всегда false. Поставь флаг в сортировку — и
        -- обогащённая копия с требованием языка проиграет необогащённой: та
        -- займёт её место в подборке без проверки, а у первой причиной станет
        -- duplicate вместо настоящей. Так и вышло при первой сборке с этими
        -- правилами 15.09.2026: из 4 вакансий с требованием языка 3 ушли как
        -- duplicate, а их непроверенные копии встали в очередь.
        -- false сортируется раньше true, поэтому живые кандидаты идут первыми.
        -- Без этого свежая «Data Engineer (m/w/d)» получила бы номер 1 (и ушла
        -- как not_english), а более старая английская «Data Engineer» —
        -- номер 2 (и ушла как duplicate). Потеряли бы обе.
        -- С мёртвыми то же самое: снятая «Senior Analytics Engineer» Zynga
        -- выигрывала дедуп, её копии уходили как duplicate, а она сама —
        -- как dead, и из подборки пропадала вся группа.
        --
        -- coalesce обязателен: у вакансий без скачанной страницы is_dead null,
        -- а null при сортировке по возрастанию идёт ПЕРВЫМ, раньше false.
        -- Без coalesce непроверенная вакансия обходила бы проверенную живую.
        --
        -- Третий ключ — копия, у которой в location ИЗ ИСТОЧНИКА есть
        -- «barcelona» или «remote», с 2026-09-22. Та же беда, что с мёртвыми,
        -- только её создало правило wrong_location: Air Apps выкладывает
        -- «Product Analyst» в Амстердаме и в Барселоне, свежей была
        -- амстердамская — она выигрывала дедуп и уходила как wrong_location,
        -- барселонская уходила как duplicate. У Aircall так же проигрывала
        -- копия «France Remote» лондонской.
        -- Почему поле источника, а не is_barcelona / llm_work_mode: ответ
        -- модели есть только у победителя (см. выше про правила по ответам
        -- модели), у копий там null. Сортировка по нему снова выбирала бы
        -- «ту, что уже обогащена», а не «ту, что подходит». location есть
        -- у каждой копии с самого начала.
        --
        -- vacancy_key в конце — чтобы при одинаковом posted_at результат
        -- не менялся от прогона к прогону.
        -- company_core вместо lower(company_name) с 2026-09-20: «Gartner»
        -- и «Gartner, Inc.» — одна компания. Для компании, от которой после
        -- нормализации не осталось ничего, company_core равен null; в
        -- partition by все null попадают в ОДНУ группу, поэтому подстрахованы
        -- coalesce на исходное название — иначе такие вакансии схлопнулись
        -- бы между собой.
        row_number() over (
            partition by coalesce(company_core, lower(company_name)),
                         coalesce(title_key, title_normalized)
            order by
                coalesce(is_dead, false),
                (seniority in ('lead', 'junior') or not is_english or is_non_data_title),
                not regexp_contains(lower(coalesce(location, '')), r'barcelona|remote'),
                posted_at desc,
                vacancy_key
        )                                               as dedup_rank

    from wrong_location_rule

),

enrichment_rule as (

    -- КТО ИДЁТ К МОДЕЛИ. Правила исключения бывают двух видов:
    --   - проверяемые без модели: грейд, роль, заголовок не про данные,
    --     мёртвая страница, дубль. Ответ на них есть у каждой вакансии
    --     сразу после сбора;
    --   - зависящие от ответа модели или от языка: awaiting_language_check
    --     (до 2026-09-22 — not_english), foreign_language_required,
    --     residency_too_long, wrong_location.
    -- Кандидат на обогащение — вакансия, прошедшая правила ПЕРВОГО вида.
    -- Правила второго вида кандидатов не отсекают: иначе модель никогда не
    -- увидит вакансию, про которую как раз она и должна ответить. Так было
    -- до 2026-09-22 — к модели шла только готовая подборка, и испанские
    -- вакансии в Барселоне отсекал not_english раньше, чем модель успевала
    -- их прочитать.
    --
    -- Флаг живёт здесь, а не в скриптах: условие раньше собирали у себя и
    -- enrich/with_gemini.py, и fetch/adzuna_pages.py, каждый своё. Теперь
    -- оба читают mart_enrichment_candidates, а та — только этот флаг.
    --
    -- Флагов два, и второй не заменяет первый:
    --   is_enrichment_candidate — прошла правила первого вида. По нему
    --     fetch/adzuna_pages.py качает страницы;
    --   is_ready_for_enrichment — кандидат, у которого есть полный текст.
    --     По нему enrich/with_gemini.py отправляет вакансию модели.
    -- Условие про полный текст нельзя класть в первый флаг: у свежей
    -- вакансии Adzuna страницы ещё нет, значит она не стала бы кандидатом,
    -- и скрипт страниц её бы не скачал — круг замкнулся бы, и новые
    -- вакансии Adzuna не обогащались бы никогда.
    --
    -- Почему модели нужен полный текст: API Adzuna обрезает описание на
    -- 500 символах, а ответ модели на обрывок записывается с версией
    -- промпта и повторно не запрашивается. Такая вакансия подождёт день и
    -- пойдёт к модели со страницей. У остальных источников описание в API
    -- полное, страниц у них нет — им условие не нужно.
    --
    -- coalesce на is_dead: у вакансии без скачанной страницы он null, а
    -- not null — тоже null, и строка выпала бы из кандидатов. То есть
    -- страницу не скачали бы именно потому, что её ещё не скачали.
    select
        *,
        seniority not in ('lead', 'junior')
            and role_type != 'other'
            and not is_non_data_title
            and not coalesce(is_dead, false)
            and dedup_rank = 1                          as is_enrichment_candidate

    from ranked

),

enrichment_ready as (

    -- Отдельный блок, потому что в одном select нельзя сослаться на
    -- колонку, которая в нём же и вычисляется.
    select
        *,
        is_enrichment_candidate
            and (source != 'adzuna' or description_source = 'page')
                                                        as is_ready_for_enrichment

    from enrichment_rule

)

select
    vacancy_key,
    source,
    source_id,
    posted_at,
    ingested_at,
    title,
    title_normalized,
    title_key,
    company_name,
    -- Нормализованное название — вторая половина ключа дедупа. В подборку
    -- не идёт, но без него не разобрать, почему две вакансии слиплись.
    company_core,
    location,
    is_remote,
    salary_min,
    salary_max,
    salary_text,
    url,
    tags,
    job_types,
    description,
    -- Чистое описание из API. По нему считается is_english.
    description_clean,
    -- Лучший доступный текст: полный со страницы, если он есть, иначе
    -- description_clean. Его читает скрипт обогащения (enrich/with_gemini.py).
    description_best,
    -- Откуда взят description_best: 'page' или 'api'.
    description_source,

    role_type,
    seniority,
    is_english,
    is_russian,
    location_fit,
    is_barcelona,
    is_agency,
    is_non_data_title,

    -- Итоговые факты (блок facts): поле источника, а где его нет — модель.
    work_mode,
    location_city,
    required_languages,
    has_language_check,
    has_source_facts,
    src_last_seen_at,
    src_remote_percentage,
    salary_min_best,
    salary_max_best,
    salary_currency_best,
    salary_period_best,
    src_required_languages,

    llm_work_mode,
    llm_location_city,
    llm_residency_requirement,
    llm_required_languages,
    llm_residency_years_required,
    fits_location,
    is_foreign_language_required,
    is_residency_too_long,
    is_wrong_location,
    is_awaiting_language_check,

    -- Номер копии в группе дублей и признак мёртвой страницы. Нужны снаружи,
    -- чтобы по витрине было видно, из чего сложился is_enrichment_candidate.
    dedup_rank,
    is_dead,
    is_enrichment_candidate,
    is_ready_for_enrichment,

    -- Первое сработавшее правило. Если не сработало ни одно, case без else
    -- вернёт null — это и значит «вакансия проходит в подборку».
    --
    -- Сверху правила без модели, ниже — зависящие от модели и языка.
    --
    -- not_english с 2026-09-22 нет: язык объявления больше не исключает.
    -- Вместо него awaiting_language_check — «на другом языке и модель ещё
    -- не проверила, какой язык требуют» (блок language_rule). Стоит сразу
    -- после правил без модели, а не на месте not_english (второй строкой):
    -- удержание — временное состояние, и испанская вакансия с чужой ролью
    -- должна уйти как irrelevant_role, навсегда, а не висеть в ожидании.
    -- Заодно число awaiting_language_check честно равно «сколько ждёт
    -- модели и потом, возможно, придёт в подборку».
    --
    -- dead — страница вакансии отдала 404 (stg_vacancy_pages). У вакансий без
    -- скачанной страницы is_dead null: when null не срабатывает, как false.
    --
    -- non_data_role — заголовок продаж, работы с клиентами или найма
    -- (is_non_data_title). После dead, чтобы у остальных вакансий причина
    -- исключения не поменялась.
    --
    -- foreign_language_required и residency_too_long — правила по полям
    -- промпта v5 (is_foreign_language_required, is_residency_too_long). В конец
    -- по той же причине: у вакансий, которые уже исключало другое правило,
    -- причина остаётся прежней.
    --
    -- wrong_location — «не Барселона и не полная удалёнка» (блок
    -- wrong_location_rule). Стоит последним по той же договорённости, и
    -- здесь у неё есть ещё одна польза: раз ни у одной вакансии причина не
    -- поменялась, множество строк с wrong_location в точности равно тому,
    -- что правило унесло из подборки. Один к одному, без примеси уже
    -- отсеянных, — именно это и хотелось увидеть при вводе правила.
    -- Поднять его выше — правка в одну строку, если когда-нибудь важнее
    -- станет читать географию раньше остальных причин.
    case
        when seniority in ('lead', 'junior')    then 'wrong_seniority'
        when dedup_rank > 1                     then 'duplicate'
        when role_type = 'other'                then 'irrelevant_role'
        when is_dead                            then 'dead'
        when is_non_data_title                  then 'non_data_role'
        when is_awaiting_language_check         then 'awaiting_language_check'
        when is_foreign_language_required       then 'foreign_language_required'
        when is_residency_too_long              then 'residency_too_long'
        when is_wrong_location                  then 'wrong_location'
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

from enrichment_ready
