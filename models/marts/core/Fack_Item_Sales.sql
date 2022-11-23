{{
    config(
        materialized='incremental',
        unique_key='ID_ITEM_SALES'
    )
}}

with stg_item_sales as (
    select * from {{ ref('stg_item_sales') }}
),

base_events1 as (
    select ORDER_ID as Order_id_events from {{ ref('base_events') }}
),

base_promos1 as (
    select * from {{ ref('base_promos') }}
),

base_users1 as (
    select ID_DIM_USERS,NK_users from {{ ref('base_users') }}
),

base_addresses1 as (
    select ID_SHIPPING_ADDRESS,NK_address from {{ ref('base_addresses') }}
)

select 
        o.ID_ITEM_SALES,
        o.FK_DIM_PRODUCTS,
        o.Quantity_sold,
        o.Load_Timestamp,
        o.DD_order_id,
        a.ID_SHIPPING_ADDRESS as FK_SHIPPING_ADDRESS,
        p.ID_DIM_promos,
        u.ID_DIM_USERS as FK_DIM_USERS,
        o.Received_at_Timestamp,
        o.Total_base_price_in_dollars,
        (o.Shipping_ponderate_cost_in_dollars+o.Total_base_price_in_dollars-p.Order_discount_in_Dollars*o.Total_base_price_in_dollars/o.ORDER_COST_IN_DOLLARS) as Total_price_in_dollars,
        (o.Total_base_price_in_dollars-p.Order_discount_in_Dollars*o.Total_base_price_in_dollars/o.ORDER_COST_IN_DOLLARS) as Total_price_before_shipping,
        (p.Order_discount_in_Dollars*o.Total_base_price_in_dollars/o.ORDER_COST_IN_DOLLARS) as Proportional_Discount_in_dollars
from stg_item_sales o
left join base_events1 e
on o.DD_order_id=e.Order_id_events
inner join base_promos1 p
on o.FK_promo_id=p.Promotion_Name
inner join base_addresses1 a 
on o.FK_address_id=a.NK_address
inner join base_users1 u 
on o.FK_user_id=NK_users


{% if is_incremental() %}

  where o.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}