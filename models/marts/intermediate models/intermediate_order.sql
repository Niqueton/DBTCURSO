with stg_order_general as (
    select * from {{ ref('stg_order_general') }}
)

select 
    md5(concat(Number_of_different_items,SHIPPING_SERVICE,RANGE_ORDER_TOTAL_USD)) as ID_DIM_ORDERS,
    NK_orders,
    Number_of_different_items,
    SHIPPING_SERVICE,
    RANGE_ORDER_TOTAL_USD
from 
stg_order_general


