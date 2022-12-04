{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_ORDERS',
    tags=['GOLD','INCREMENTAL']
    ) 
    }}

with intermediate_order as (
    select * from {{ ref('intermediate_order') }}
)

select 
    ID_DIM_ORDERS,
    Number_of_different_items,
    Total_number_of_items,
    Status,
    SHIPPING_SERVICE,
    RANGE_ORDER_TOTAL_USD
from 
intermediate_order

{% if is_incremental() %}

  where ID_DIM_ORDERS =! (select ID_DIM_ORDERS from {{ this }})

{% endif %}

union 

select  
     '0'
    , 0
    , 0
    , 'No aplica'
    , 'No aplica'
    , 'No aplica'
