{% macro fecha_id(date1) %}

(year({{ date1 }})*10000+month({{ date1 }})*100+day({{ date1 }}))

{% endmacro %}