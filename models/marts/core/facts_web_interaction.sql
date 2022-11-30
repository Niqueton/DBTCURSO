with base_events as (
    select * from {{ ref('base_events') }}
),
intermediate_order as (
    select * from {{ ref('intermediate_order') }}
),
dim_customer as (
    select * from {{ ref('dim_customer_historica') }}
)

    select 
        e.ID_WEB_INTERACTION,
        e.Produced_at_Date,
        e.Produced_at_Time,
        e.EVENT_ID,
        e.EVENT_TYPE,
        o.ID_DIM_ORDERS
        e.ORDER_ID,
        e.PAGE_URL,
        e.PRODUCT_ID,
        e.SESSION_ID,
        c.ID_DIM_CUSTOMER,
        e.Load_Timestamp
    from base_events e 

    left join intermediate_order o 
    on e.ORDER_ID=o.NK_orders

    left join dim_customer c 
    on e.USER_ID=c.NK_customer


  