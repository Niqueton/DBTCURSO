{% macro nulos_a_desconocidos(table, column) %}
 
    update {{ table }} set {{ column }} ='Desconocido' where {{ column }} is null

{% endmacro %}