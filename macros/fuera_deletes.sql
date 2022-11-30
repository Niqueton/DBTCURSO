{% macro fuera_deletes(tabla,unique_column) %}

borrados as (
select {{unique_column}} as Aux from {{tabla}}
where _fivetran_deleted=TRUE
),

ui as (
select * from {{tabla}} a
left join borrados b 
on a.{{unique_column}}=b.Aux
where b.Aux is null
)

select *exclude(Aux,_fivetran_deleted) from ui



{% endmacro %}