{{ config(
    materialized='incremental',
    unique_key = 'ID_WEB_INTERACTION',
    tags=['FACT_TABLE','INCREMENTAL']
    ) 
    }}


with stg_events as (
    select * from {{ ref('stg_events') }}
),
intermediate_order as (
    select * from {{ ref('intermediate_order') }}
),
dim_customer as (
    select * from {{ ref('dim_customer_historica') }}
),
dim_products as (
    select * from {{ ref('dim_products_historica') }}
),
intermediate_session as (
    select * from {{ ref('intermediate_session') }}
)


    select 

        {{ fecha_id('e.Produced_at_date')}} as ID_PRODUCED_AT_DATE,
        {{ time_id('e.Produced_at_time')}} as ID_PRODUCED_AT_TIME,
        e.NK_events,
        e.EVENT_TYPE,
        {{ cambio_valor('o.ID_DIM_ORDERS',"null","'0'") }} as ID_DIM_ORDERS,
        {{ cambio_valor('e.NK_orders',"null","'No aplica'") }} as DD_Order,
        e.PAGE_URL,
        {{ cambio_valor('p.ID_DIM_products',"null",'0') }} as ID_DIM_products,
        s.ID_DIM_session,
        c.ID_DIM_CUSTOMER,
        e.Load_Timestamp

    from stg_events e 

    left join intermediate_order o 
    on e.NK_orders=o.NK_orders

    left join dim_customer c 
    on e.NK_users=c.NK_customer

    left join intermediate_session s 
    on e.DD_session=s.DD_session

    left join dim_products p 
    on e.NK_products=p.NK_products

{% if is_incremental() %}

  where _fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}
  