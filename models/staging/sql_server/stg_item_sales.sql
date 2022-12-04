
{{ config(
    materialized='incremental',
    unique_key = 'ID_ITEM_SALES',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}


with base_orders1 as (
    select * from {{ ref('base_orders_snapshot') }}
),

base_order_items1 as (
    select * from {{ ref('base_order_items') }}
),

base_products1 as (
    select * from {{ ref('base_products') }}
),

stg_promos_snapshot as (
    select * from {{ ref('stg_promos_snapshot') }}
)


    select
        oi.ID_ITEM_SALES,
        oi.NUMBER_OF_UNITS as Quantity_sold,
        p.ID_DIM_products,
        oi.Load_Timestamp,
        o.NK_orders,
        o.NK_address,
        ps.ID_DIM_promos,
        o.NK_users,
        o.Received_at_Timestamp,
        (p.Product_base_Price*oi.NUMBER_OF_UNITS) as Total_base_price_USD,
        ps.Order_discount_USD,
        o.ORDER_COST_USD

    from base_orders1 as o

    left join base_order_items1 as oi 
    on o.NK_orders=oi.NK_orders

    inner join base_products1 as p 
    on oi.NK_products=p.NK_products

    left join stg_promos_snapshot as ps 
    on o.Promotion_Name=ps.Promotion_Name
{% if is_incremental() %}

  where oi.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}