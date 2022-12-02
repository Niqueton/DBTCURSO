with intermediate_order as (
    select * from {{ ref('intermediate_order') }}
)

select 
    md5(concat(Number_of_different_items,SHIPPING_SERVICE,RANGE_ORDER_TOTAL_USD)) as ID_DIM_ORDERS,
    Number_of_different_items,
    Total_number_of_items,
    Status,
    SHIPPING_SERVICE,
    RANGE_ORDER_TOTAL_USD
from 
intermediate_order

union 

select  
     '0'
    , 0
    , 0
    , 'No aplica'
    , 'No aplica'
