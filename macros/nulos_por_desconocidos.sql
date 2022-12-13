{% macro nulos_por_desconocidos(model,columns) %}
 

   {% set query %}
      create or replace table {{ model }} as
      (select 
        case 
         when {{ columns }} is null then 'Desconocido'
         else {{ columns }}
        end as concat('{{columns}}',_arreglada)
      from {{ model }})
   {% endset %}

   {% do run_query(query) %}


{% endmacro %}


