{% macro scd_tipo1(tabla,unique_column) %}

previo as(
    select
     max(_fivetran_synced) as Bueno,{{ unique_column }} as Pepon from   {{ tabla }}
    where _fivetran_deleted is null group by {{ unique_column }})
),
todo as (
select * from {{ tabla }} a
inner join previo b 
on a.{{ unique_column }}=b.Pepon
and a._fivetran_synced=b.Bueno
)

select *exclude(Bueno,Pepon) from todo
{% endmacro %}