{% macro cambio_valor(columna,actual,cambio) %}

case

    when ifnull({{ columna }},'pepon')=ifnull({{ actual }},'pepon') then {{ cambio }}
    else {{ columna }}

end

{% endmacro %}