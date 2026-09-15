-- Слой staging для журнала исходов отправки в Telegram.
--
-- В raw.digest_sent строку пишет бот: одна строка = одна вакансия с
-- окончательным исходом — sent (доставлена) или undeliverable (Telegram
-- отказал с кодом 400, повторять бессмысленно). Таблица только дописывается.
-- Если бот запишет вакансию ещё раз (например, после перезапуска), в raw
-- окажутся две строки, а здесь останется одна.
--
-- ГРЕЙН: одна вакансия (vacancy_key). Тест unique в _staging.yml его
-- сторожит: mart_digest_queue по этой модели убирает из очереди всё, у чего
-- уже есть исход.
--
-- qualify row_number, как в stg_enrichment: нужна одна строка целиком —
-- статус вместе с датой. Пока статуса не было, хватало group by и min(sent_at).

with digest_sent as (

    select
        vacancy_key,

        -- null — строка записана до появления колонки status. Тогда бот
        -- писал только доставленное, так что null — это sent. Проставить
        -- этим строкам sent в самой таблице нельзя: raw только дописывается.
        coalesce(status, 'sent')                    as status,

        sent_at
    from {{ source('raw', 'digest_sent') }}

)

select
    vacancy_key,
    status,
    sent_at
from digest_sent

-- Из нескольких строк вакансии оставляем первую доставку, если она была:
-- доставленная хоть раз вакансия — sent. status = 'sent' по убыванию ставит
-- true первым. Доставок не было — остаётся первая запись undeliverable.
qualify row_number() over (
    partition by vacancy_key
    order by status = 'sent' desc, sent_at
) = 1
