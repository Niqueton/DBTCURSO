{% macro cambio_valor(columna,actual,cambio) %}


case
    when ifnull({{ columna }},'-1')=ifnull({{ actual }},'-1') then {{ cambio }}
    else {{ columna }}
end

{% endmacro %}