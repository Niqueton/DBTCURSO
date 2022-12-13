{{ config(
    materialized='incremental',
    unique_key=['NK_products','id_anio_mes_prediccion'],
    tags=['GOLD','INCREMENTAL','FACT_TABLE']
    ) 
    }}

with base_budget as (
    select * from {{ ref('base_budget') }}
),
intermediate_sb1 as (
    select * from {{ ref('intermediate_stockandbudget') }}
)
,
intermediate_sb2 as (
    select * from {{ ref('intermediate_stockandbudget') }}
)
,
dim_products_actual as (
    select NK_products,Product_base_Price from {{ ref('dim_products_actual') }}

)

select

b.NK_products,
b.NUMBER_OF_UNITS_EXPECTED as Presupuesto_empresa,
sb1.Prevision_lineal_ventas,
b.id_anio_mes_prediccion,
sb2.Unidades_mes_pasado as Unidades_vendidas,
(p.Product_base_Price*b.NUMBER_OF_UNITS_EXPECTED) as  Presupuesto_empresa_USD,
(p.Product_base_Price*(-b.NUMBER_OF_UNITS_EXPECTED+sb2.Unidades_mes_pasado)) as Balance_respecto_presupuesto_USD,
(p.Product_base_Price*(-sb1.Prevision_lineal_ventas+sb2.Unidades_mes_pasado)) as Balance_respecto_prevision_lineal_USD,
b.Load_Date

from base_budget b 

left join intermediate_sb1 sb1 
on b.NK_products=sb1.NK_products
and b.id_anio_mes_anterior=sb1.id_anio_mes_anterior

left join intermediate_sb2 sb2 
on b.NK_products=sb2.NK_products
and b.id_anio_mes_prediccion=sb2.id_anio_mes_anterior

left join dim_products_actual p 
on b.NK_products=p.NK_products


{% if is_incremental() %}

  where b.Load_date >= (select max(Load_date) from {{ this }})

{% endif %}