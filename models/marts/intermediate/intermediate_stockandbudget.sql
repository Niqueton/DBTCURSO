{{
    config(
        materialized='incremental',
        unique_key=['NK_products','id_anio_mes'],
        tags=['INCREMENTAL','INTERMEDIATE']
    )
}}


with order_items as (
    select * from {{ ref('facts_item_sales') }}
),
dim_products_historica as (
    select ID_DIM_products,NK_products from {{ ref('dim_products_historica') }}
),

primer as (

select 

p.NK_products,
o.Quantity_sold,
round((o.id_produced_date/100),0) as id_anio_mes,
substring(id_anio_mes,5)::number as mes

from order_items o 

left join dim_products_historica p 
on o.ID_DIM_products=p.ID_DIM_products

{% if is_incremental() %}

  where o.ID_PRODUCED_DATE/100 >=(select max(id_anio_mes) from {{ this }})

{% endif %}
)
,

segundo as (
    select 
    NK_products,
    sum(Quantity_sold) as Unidades_vendidas,
    id_anio_mes,
    case 
    when mes<12
        then (id_anio_mes+1) 
    else
        (id_anio_mes+89) 
   
    end as id_anio_mes_next,
    case
    when mes=01
        then (id_anio_mes-89)
    else 
        (id_anio_mes-1)
    end as id_anio_mes_last

    from primer
    group by NK_products,id_anio_mes,mes
),
tercero as (
select * from segundo
),
cuarto as (

select 

m2.NK_products,
m2.id_anio_mes,
m2.id_anio_mes_next,
m2.id_anio_mes_last,
m2.Unidades_vendidas,
m3.Unidades_vendidas as Unidades_anterior,
(m2.Unidades_vendidas-m3.Unidades_vendidas) as pendiente


from segundo m2 

inner join tercero m3 
on m2.id_anio_mes=m3.id_anio_mes_next
and m2.NK_products=m3.NK_products

),
quinto as (
    select 
    (2*pendiente+unidades_anterior) as Prevision_lineal_venta,
    NK_products,
    id_anio_mes_next as id_anio_mes,
    id_anio_mes as id_anio_mes_anterior,
    Unidades_vendidas as Unidades_mes_pasado

    from 
    cuarto
)

select 
NK_products,
id_anio_mes,
id_anio_mes_anterior,
Unidades_mes_pasado,
case
when Prevision_lineal_venta<0 then 0
else Prevision_lineal_venta
end as Prevision_lineal_ventas
 from quinto

