{{
    config(
        materialized='incremental',
        unique_key='ID_ITEM_SALES',
        tags=['INCREMENTAL','FACT_TABLE']
    )
}}

with stg_item_sales as (
    select * from {{ ref('stg_item_sales') }}
),

stg_events as (
    select NK_orders,DD_session from {{ ref('stg_events') }}
),

intermediate_session as (
    select DD_session,ID_DIM_session from {{ ref('intermediate_session') }}
),

dim_customer_historica as (
    select ID_DIM_customer,NK_customer from {{ ref('dim_customer_historica') }}
),

dim_shipping_addresses as (
    select ID_DIM_SHIPPING_ADDRESS,NK_address from {{ ref('dim_shipping_addresses') }}
),

intermediate_order as (
    select NK_orders,ID_DIM_ORDERS from {{ ref('intermediate_order') }}
),

dim_products_historica as (
    select ID_DIM_products,NK_products from {{ ref('dim_products_historica') }}
)

select 
        o.ID_ITEM_SALES,
        io.ID_DIM_ORDERS,
        p.ID_DIM_products,
        a.ID_DIM_SHIPPING_ADDRESS,
        c.ID_DIM_CUSTOMER,
        o.ID_DIM_promos,
        s.ID_DIM_session,
        o.Quantity_sold,
        o.Load_Timestamp,
        case
            when e.NK_orders is null then 'Tienda física'
            else 'Página web'
        end as Metodo_compra,
        o.NK_orders as DD_ORDERS,
       {{ fecha_id('to_date(o.Received_at_Timestamp)') }} as ID_PRODUCED_DATE,
       {{ time_id('to_time(o.Received_at_Timestamp)')}} as ID_PRODUCED_TIME,
        o.Total_base_price_USD,
        (o.Total_base_price_USD*(1-o.Order_discount_USD/o.ORDER_COST_USD)) as Final_revenue_USD,
        (o.Total_base_price_USD*o.Order_discount_USD/o.ORDER_COST_USD) as Proportional_discount_USD


from stg_item_sales o

left join stg_events e
on o.NK_orders=e.NK_orders

left join dim_customer_historica c
on o.NK_users=c.NK_customer

left join dim_shipping_addresses a 
on o.NK_address=a.NK_address

left join intermediate_order io 
on o.NK_orders=io.NK_orders 

left join intermediate_session s 
on e.DD_session=s.DD_session

inner join dim_products_historica p 
on o.NK_products=p.NK_products

{% if is_incremental() %}

  where o.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}