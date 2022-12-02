with base_events as (
    select * from {{ ref('base_events') }}
),
intermediate_order as (
    select * from {{ ref('intermediate_order') }}
),
dim_customer as (
    select * from {{ ref('dim_customer_historica') }}
),
dim_products as (
    select * from {{ ref('dim_products_historica') }}
)

    select 
        e.ID_WEB_INTERACTION,
        {{ fecha_id('e.Produced_at_date')}} as ID_PRODUCED_AT_DATE,
        {{ time_id('e.Produced_at_time')}} as ID_PRODUCED_AT_TIME,
        e.EVENT_ID,
        e.EVENT_TYPE,
        o.ID_DIM_ORDERS
        e.ORDER_ID as DD_Order_id,
        e.PAGE_URL,
        p.ID_DIM_products,
        e.SESSION_ID  as DD_Session_id,
        c.ID_DIM_CUSTOMER,
        e.Load_Timestamp
    from base_events e 

    left join intermediate_order o 
    on e.ORDER_ID=o.NK_orders

    left join dim_customer c 
    on e.USER_ID=c.NK_customer

    left join dim_products p 
    on e.PRODUCT_ID=p.NK_product


  