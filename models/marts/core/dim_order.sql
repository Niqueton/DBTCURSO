with intermediate_order as (
    select * from {{ ref('intermediate_order') }}
)

select 
    md5(concat(Number_of_different_items,SHIPPING_SERVICE,RANGE_ORDER_TOTAL_USD)) as ID_DIM_ORDERS,
    Number_of_different_items,
    SHIPPING_SERVICE,
    RANGE_ORDER_TOTAL_USD
from 
stg_order_general
