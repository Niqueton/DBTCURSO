{% macro pasar_nulos_a_desconcidos() %}

{% set query %}
begin
create or replace function null_to_unknow(campo varchar)
return varchar

as
'begin
select 
case 
    when {{ campo }} is null then 'Unknow'
    else {{ campo }}
end as '{{campo}}'
end'
end
{% endset %}

{% do run_query(query) %}

{% endmacro %}