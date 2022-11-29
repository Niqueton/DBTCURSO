with stg_item_sales as (
    select * from {{ ref('stg_item_sales') }}
),

stg_order_tracking as (
    select * from {{ ref('stg_order_tracking') }}
)

select 
    o.NK_orders,
    count(s.FK_DIM_PRODUCTS) as Number_of_different_items,
    o.SHIPPING_SERVICE,
    case
        when o.ORDER_TOTAL_IN_DOLLARS < 25 then '0-25 USD'
        when o.ORDER_TOTAL_IN_DOLLARS < 50 then '25-50 USD'
        when o.ORDER_TOTAL_IN_DOLLARS < 75 then '50-75 USD'
        when o.ORDER_TOTAL_IN_DOLLARS < 100 then '75-100 USD'
        when o.ORDER_TOTAL_IN_DOLLARS < 125 then '100-125 USD'
        when o.ORDER_TOTAL_IN_DOLLARS < 150 then '125-150 USD'
        else '> 150 USD'
    end as RANGE_ORDER_TOTAL_USD
from 
stg_order_tracking o 
left join 
stg_item_sales s 
on o.NK_orders=s.DD_order_id
group by 
o.NK_orders,o.SHIPPING_SERVICE,o.ORDER_TOTAL_IN_DOLLARS