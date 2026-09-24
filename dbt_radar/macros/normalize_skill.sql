{#
    Навык из поля stack (ответ модели) к одному написанию.

    Зачем: модель пишет один и тот же инструмент по-разному — «Apache
    Airflow» и «Airflow», «PowerBI» и «Power BI», «Google BigQuery» и
    «BigQuery». Без склейки спрос на навык делится на несколько строк, и
    каждая выглядит меньше, чем есть. Замерено 2026-09-24: «apache airflow»
    20 раз рядом с «airflow» 93, «powerbi» 16 рядом с «power bi» 78.

    Где используется:
      mart_skill_demand — навык в зерне витрины.

    Правило: склеиваем только НАПИСАНИЯ одного и того же инструмента
    (решение 2026-09-24). Родственные, но разные навыки остаются разными:
      — pyspark ≠ spark: это Python-API к Spark, а не сам Spark;
      — postgresql, mysql, t-sql ≠ sql: конкретная СУБД или диалект, а не
        язык запросов вообще;
      — looker studio ≠ looker: разные продукты Google;
      — ga4 ≠ google analytics: версия продукта, её стоит видеть отдельно.
    Словарь открытый: новое написание, которое встретится в stack,
    дописывается сюда, и витрина пересчитывается сама.

    Сначала lower и trim: «SQL» и «sql », «Python» и «python» — одно и то же
    без всякого словаря. Всё, чего нет в словаре, возвращается в этом виде.
#}
{% macro normalize_skill(column) %}
    case lower(trim({{ column }}))
        when 'apache airflow'           then 'airflow'
        when 'apache spark'             then 'spark'
        when 'apache kafka'             then 'kafka'
        when 'apache flink'             then 'flink'
        when 'apache iceberg'           then 'iceberg'
        when 'powerbi'                  then 'power bi'
        when 'ms power bi'              then 'power bi'
        when 'microsoft power bi'       then 'power bi'
        when 'google bigquery'          then 'bigquery'
        when 'google cloud bigquery'    then 'bigquery'
        when 'big query'                then 'bigquery'
        when 'google cloud platform'    then 'gcp'
        when 'google cloud'             then 'gcp'
        when 'microsoft azure'          then 'azure'
        when 'amazon web services'      then 'aws'
        when 'amazon s3'                then 's3'
        when 'postgres'                 then 'postgresql'
        when 'microsoft sql server'     then 'sql server'
        when 'ms sql server'            then 'sql server'
        when 'mssql'                    then 'sql server'
        when 'mssql server'             then 'sql server'
        when 'dbt core'                 then 'dbt'
        when 'dbt cloud'                then 'dbt'
        when 'k8s'                      then 'kubernetes'
        when 'microsoft excel'          then 'excel'
        when 'ms excel'                 then 'excel'
        else lower(trim({{ column }}))
    end
{% endmacro %}
