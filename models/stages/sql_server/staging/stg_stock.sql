
{{ config(
    materialized='incremental',
    unique_key=['NK_products','id_anio_mes'],
    tags=['SILVER','INCREMENTAL']
    ) 
    }}

with base_products as (
    select * from {{ ref('base_products') }}
)

select

  NK_products,
  INVENTORY as Stock,
  year(Load_date)*100+month(Load_date) as id_anio_mes

from base_products


{% if is_incremental() %}

  where year(Load_date)*100+month(Load_date)+day(Load_date)/100 > (select max(id_anio_mes) from {{ this }})

{% endif %}