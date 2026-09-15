-- Слой staging для страниц вакансий, которые скачивает fetch/adzuna_pages.py.
--
-- В raw.vacancy_pages строка — это попытка скачать страницу, и на одну
-- вакансию их бывает несколько: 403 или таймаут в один прогон, 200 в
-- следующий. Сырой слой хранит все попытки, а здесь по вакансии остаётся
-- последняя.
--
-- ГРЕЙН: одна вакансия (vacancy_key). Тест unique в _staging.yml его
-- сторожит: stg_vacancies и mart_vacancies_scored джойнят эту модель, и
-- дубль здесь размножил бы строки там.

with pages as (

    select
        vacancy_key,
        http_status,
        fetched_at,

        -- Мёртвой считаем вакансию, у которой последняя попытка вернула 404:
        -- страница снятой вакансии отдаёт именно этот код. По возрасту судить
        -- нельзя — одни вакансии на Adzuna снимают через несколько дней,
        -- другие висят неделями.
        -- coalesce: у попытки без ответа (сетевая ошибка, пропуск /land/ad/)
        -- http_status null, и сравнение дало бы null. Нет ответа — значит
        -- «не проверили», а не «снята», поэтому false.
        coalesce(http_status = 404, false)                  as is_dead,

        -- Когда вакансию последний раз УСПЕШНО проверили. Успешная проверка —
        -- окончательный ответ: код 200 с текстом страницы или код 404. То же
        -- определение, что в fetch/adzuna_pages.py: 403, таймаут и сетевая
        -- ошибка о живости вакансии ничего не говорят.
        --
        -- Оконная функция, а не колонка последней попытки: последней может
        -- оказаться 403, а успешная проверка — вчерашней. max по всем попыткам
        -- вакансии (partition by vacancy_key) её найдёт, а qualify ниже всё
        -- равно оставит одну строку.
        -- if(...) даёт null у неуспешных попыток, max их пропускает. Успешных
        -- не было вовсе — last_checked_at null.
        max(if(
            (http_status = 200 and page_text is not null) or http_status = 404,
            fetched_at,
            null
        )) over (partition by vacancy_key)                  as last_checked_at,

        -- Та же чистка, что у description_clean в stg_vacancies: макрос
        -- macros/clean_html_text.sql. page_text заполнен только у живой
        -- страницы (код 200 и разметка JobPosting); у остальных попыток он
        -- null — и page_text_clean тоже.
        -- nullif: если после чистки не осталось ни символа, текста нет. Иначе
        -- пустая строка вытеснила бы в description_best описание из API.
        nullif({{ clean_html_text('page_text') }}, '')      as page_text_clean

    from {{ source('raw', 'vacancy_pages') }}

)

select *
from pages

-- Та же техника, что в stg_enrichment: пронумеровать попытки внутри вакансии
-- от свежей к старой и оставить первую.
qualify row_number() over (
    partition by vacancy_key
    order by fetched_at desc
) = 1
