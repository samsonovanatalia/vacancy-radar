-- Витрина для дашборда: зарплатные вилки.
--
-- ГРЕЙН: одна роль × один грейд × один город × один период оплаты × одна
-- валюта.
--
-- НАСЕЛЕНИЕ — тот же рынок, что у mart_skill_demand (is_market_vacancy),
-- а не подборка: грейд, язык, локация и отправка не фильтруют.
--
-- ЗАРПЛАТА — по тому же правилу, что везде: вилка, которую указал
-- источник, главнее найденной моделью (колонки salary_*_best в
-- mart_vacancies_scored). Правило живёт там; здесь его не повторяем.
--
-- Период оплаты в зерне, а не пересчёт к одной шкале. Годовую и месячную
-- сумму можно было бы свести, умножив на 12, но в Испании зарплату платят
-- 12 или 14 раз в год, и какое число подставить — допущение, которого в
-- данных нет. Разные периоды — разные строки, сравнивать их — дело читателя.
--
-- Валюта тоже в зерне. Город её не определяет: у удалёнки города нет, и в
-- одной ячейке оказались бы евро, фунты и доллары. Медиана смеси валют —
-- число, которое ничего не значит.
--
-- city, salary_period и salary_currency бывают null (город не указан,
-- у Adzuna без ответа модели нет ни периода, ни валюты). Такие вакансии
-- не выбрасываем: null — отдельная группа, «не указано».
--
-- ОГРАНИЧЕНИЕ: окно mart_vacancies_scored — 30 дней. Это срез текущего
-- рынка, а не история.

with salaries as (

    select
        vacancy_key,
        role_type,
        seniority,
        location_city                                   as city,
        salary_period_best                              as salary_period,
        salary_currency_best                            as salary_currency,
        salary_min_best                                 as salary_min,
        salary_max_best                                 as salary_max
    from {{ ref('mart_vacancies_scored') }}
    where is_market_vacancy
      -- Вилка указана, если есть хотя бы одна граница: «от 50 000» —
      -- тоже информация о рынке.
      and (salary_min_best is not null or salary_max_best is not null)

),

medians as (

    -- percentile_cont в BigQuery — только оконная функция, агрегатной
    -- медианы нет. Поэтому медиану считаем окном по группе грейна, а
    -- ниже group by сворачивает одинаковые значения в одну строку.
    -- Альтернатива approx_quantiles — агрегатная, но приблизительная; в
    -- ячейках по 1–3 вакансии приближение видно глазом.
    -- null-границы percentile_cont пропускает: медиана нижней границы — по
    -- вакансиям, где нижняя граница указана.
    select
        *,
        percentile_cont(salary_min, 0.5) over grain_window  as salary_min_median,
        percentile_cont(salary_max, 0.5) over grain_window  as salary_max_median
    from salaries
    window grain_window as (
        partition by role_type, seniority, city, salary_period, salary_currency
    )

)

select
    -- Ключ строки для тестов unique и not_null. coalesce: concat с null
    -- даёт null, а null-группы — законные строки.
    concat(
        role_type, '|', seniority, '|',
        coalesce(city, '(не указан)'), '|',
        coalesce(salary_period, '(не указан)'), '|',
        coalesce(salary_currency, '(не указана)')
    )                                                   as salary_key,
    role_type,
    seniority,
    city,
    salary_period,
    salary_currency,
    count(*)                                            as vacancies_count,
    -- Медиана внутри группы одна и та же у всех строк — берём любую.
    any_value(salary_min_median)                        as salary_min_median,
    any_value(salary_max_median)                        as salary_max_median,
    -- Минимум — по нижним границам, максимум — по верхним: размах рынка
    -- от самого скромного «от» до самого щедрого «до».
    min(salary_min)                                     as salary_min_min,
    max(salary_max)                                     as salary_max_max

from medians
group by role_type, seniority, city, salary_period, salary_currency
