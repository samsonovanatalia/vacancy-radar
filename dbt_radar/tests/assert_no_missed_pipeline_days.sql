-- Сингулярный тест: за последние 14 полных дней не было дня без сбора или
-- без отправки (mart_pipeline_days).
--
-- Возвращает пропущенные дни. dbt build печатает их число как WARN в логе
-- каждого прогона — видно без ручного запроса, сами даты — в
-- mart_pipeline_days.
--
-- warn, а не error: пропуск уже случился, и падение сегодняшней сборки его не
-- вернёт — только оставит без дайджеста ещё и сегодня. error_if > 3: больше
-- трёх пропущенных дней из 14 — расписание сломано, задача должна стать
-- красной. Остального ошибка не остановит: от mart_pipeline_days не зависит
-- ни одна модель, а шаги collect.yml после dbt build идут по !cancelled().

{{ config(severity='error', warn_if='>0', error_if='>3') }}

select
    run_date,
    is_collect_missing,
    is_digest_missing
from {{ ref('mart_pipeline_days') }}
where is_collect_missing
   or is_digest_missing
