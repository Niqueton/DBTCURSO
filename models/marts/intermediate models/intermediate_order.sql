{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_ORDERS',
    tags=['GOLD','INCREMENTAL']
    ) 
    }}

with stg_order_tracking_snapshot as (
    select * from {{ ref('stg_order_tracking') }}
)



select 
    {{ dbt_utils.surrogate_key(['Number_of_different_items','Total_number_of_items','Status','SHIPPING_SERVICE','RANGE_ORDER_TOTAL_USD']) }} as ID_DIM_ORDERS,
    NK_orders,
    Number_of_different_items,
    Total_number_of_items,
    Status,
    SHIPPING_SERVICE,
    RANGE_ORDER_TOTAL_USD,
    Load_Timestamp
from 
stg_order_tracking_snapshot

{% if is_incremental() %}

  where Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}
