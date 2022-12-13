{% macro time_id(time1) %}

(hour({{ time1 }})*10000+minute({{ time1 }})*100+second({{ time1 }}))

{% endmacro %}