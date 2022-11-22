
{{ config(
    materialized='incremental',
    unique_key = 'ID_ITEM_SALES'
    ) 
    }}


with base_orders1 as (
    select * from {{ ref('base_orders') }}
),

base_order_items1 as (
    select * from {{ ref('base_order_items') }}
),
base_products1 as (
    select NK_product,Product_base_Price from {{ ref('base_products') }}
),

stg_item_sales as (
    select
        md5(concat(oi.PRODUCT_ID,o.NK_orders)) as ID_ITEM_SALES,
        oi.PRODUCT_ID as FK_product_id,
        oi.NUMBER_OF_UNITS as Quantity_sold,
        oi.Load_Timestamp,
        o.NK_orders as DD_order_id,
        o.ADDRESS_ID as FK_address_id,
        o.PROMO_ID as FK_promo_id,
        o.USER_ID as FK_user_id,
        o.Received_at_Timestamp,
        (p.Product_base_Price*oi.NUMBER_OF_UNITS) as Total_base_price_in_dollars,
        (p.Product_base_Price*oi.NUMBER_OF_UNITS)*o.SHIPPING_COST_IN_DOLLARS/o.ORDER_COST_IN_DOLLARS as Shipping_ponderate_cost_in_dollars

    from base_orders1 as o
    left join base_order_items1 as oi 
    on o.NK_orders=oi.ORDER_ID
    left join base_products1 as p 
    on oi.PRODUCT_ID=p.NK_product
)

select * from stg_item_sales

{% if is_incremental() %}

  where oi.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}