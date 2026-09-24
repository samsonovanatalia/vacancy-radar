{#
    Город к одному написанию.

    Зачем: город приходит из двух мест — список городов Manfred и ответ
    модели, — и один город бывает записан по-разному. Без склейки он
    делится на две строки в витринах дашборда. Замерено 2026-09-24 в
    mart_salary: «A Coruña» и «Coruña» — две строки.

    Где используется:
      mart_vacancies_scored — итоговый location_city (блок facts).

    Правило то же, что у normalize_skill: склеиваем только НАПИСАНИЯ одного
    города. Словарь открытый — новое написание дописывается сюда.
    Регистр у городов вне словаря не трогаем: «Düsseldorf» остаётся как
    есть, а lower превратил бы подписи на дашборде в «düsseldorf».
#}
{% macro normalize_city(column) %}
    case lower(trim({{ column }}))
        when 'coruña'       then 'A Coruña'
        when 'a coruña'     then 'A Coruña'
        when 'la coruña'    then 'A Coruña'
        when 'la coruna'    then 'A Coruña'
        when 'a coruna'     then 'A Coruña'
        else trim({{ column }})
    end
{% endmacro %}
