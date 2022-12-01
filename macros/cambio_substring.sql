{% macro cambio_substring(columna,actual,cambio) %}

case
    when {{ columna }}={{ actual }} then {{ cambio }}
    else {{ columna }}
end

{% endmacro %}