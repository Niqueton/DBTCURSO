{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_CUSTOMER',
    ) 
    }}

with stg_products_snapshot as (
    select * from {{ ref('stg_products_snapshot') }}
)
select 

from







{% if is_incremental() %}

  where u.DBT_VALID_FROM > (select max(VALID_FROM) from {{ this }})

{% endif %}