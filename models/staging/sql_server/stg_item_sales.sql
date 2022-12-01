
{{ config(
    materialized='incremental',
    unique_key = 'ID_ITEM_SALES',
    tags=['SILVER','Incremental']
    ) 
    }}


with base_orders1 as (
    select * from {{ ref('base_orders') }}
),

base_order_items1 as (
    select * from {{ ref('base_order_items') }}
),

base_products1 as (
    select * from {{ ref('base_products') }}
)


    select
        oi.ID_ITEM_SALES,
        oi.NUMBER_OF_UNITS as Quantity_sold,
        p.ID_DIM_products,
        oi.Load_Timestamp,
        o.NK_orders,
        o.NK_address,
        o.Promotion_Name,
        o.NK_users,
        o.Received_at_Timestamp,
        (p.Product_base_Price*oi.NUMBER_OF_UNITS) as Total_base_price_in_dollars,
        (p.Product_base_Price*oi.NUMBER_OF_UNITS)*o.SHIPPING_COST_IN_DOLLARS/o.ORDER_COST_IN_DOLLARS as Shipping_ponderate_cost_in_dollars,
        o.ORDER_COST_IN_DOLLARS

    from base_orders1 as o

    left join base_order_items1 as oi 
    on o.NK_orders=oi.NK_orders

    inner join base_products1 as p 
    on oi.NK_products=p.NK_products

{% if is_incremental() %}

  where oi.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}