{{ config(
    materialized='incremental',
    unique_key=['NK_products','id_anio_mes'],
    tags=['GOLD','INCREMENTAL']
    ) 
    }}

with stg_stock as (
    select * from {{ ref('stg_stock') }}
)
,
intermediate_sb as (
  select * from {{ ref('intermediate_stockandbudget') }}
),

dim_products_actual as (
  select NK_products,Product_base_Price from {{ ref('dim_products_actual') }}
)

select

  s.NK_products,
  s.Stock,
  s.id_anio_mes,
  case
  when sb.Prevision_lineal_ventas<=sb.Unidades_mes_pasado
    then sb.Unidades_mes_pasado
  else sb.Prevision_lineal_ventas
  end as Stock_Minimo,
  (p.Product_base_Price*s.Stock) as  Valoracion_USD


  

from stg_stock s 


left join intermediate_sb sb 
on s.NK_products=sb.NK_products
and s.id_anio_mes=sb.id_anio_mes_anterior

left join dim_products_actual p 
on s.NK_products=p.NK_products

{% if is_incremental() %}

  where s.id_anio_mes >= (select max(id_anio_mes) from {{ this }})

{% endif %}