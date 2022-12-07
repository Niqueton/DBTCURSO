{{ config(
    materialized='incremental',
    unique_key = 'DD_order',
    tags=['INCREMENTAL','FACT_TABLE']
    ) 
    }}


with stg_order_tracking as (
    select * from {{ ref('stg_order_tracking') }}
),

dim_shipping_address as (
    select * from {{ ref('dim_shipping_addresses') }}
),

intermediate_order as (
    select * from {{ ref('intermediate_order') }}
),

dim_promos as (
    select * from {{ ref('dim_promos_historica') }}
),

dim_customer as (
    select * from {{ ref('dim_customer_historica') }}
),

stg_events as (
    select * from {{ ref('stg_events') }}
),
intermediate_session as (
    select * from {{ ref('intermediate_session') }}
)



select 

      c.ID_DIM_CUSTOMER
    , p.ID_DIM_promos
    , a.ID_DIM_SHIPPING_ADDRESS
    , ot.NK_orders as DD_order
    , ot.DD_Tracking
    , {{ fecha_id('to_date(ot.Received_at_Timestamp)') }} as ID_RECEIVED_DATE
    , {{ time_id('to_time(ot.Received_at_Timestamp)') }} as ID_RECEIVED_TIME
    , {{ fecha_id('to_date(ot.Delivered_at_Timestamp)') }} as ID_DELIVERED_DATE
    , {{ time_id('to_time(ot.Delivered_at_Timestamp)') }} as ID_DELIVERED_TIME
    , {{ fecha_id('to_date(ot.Estimated_delivery_at_Timestamp)') }} as ID_ESTIMATED_DELIVERY_DATE
    , {{ time_id('to_time(ot.Estimated_delivery_at_Timestamp)') }} as ID_ESTIMATED_DELIVERY_TIME
    , s.ID_DIM_session
    , ot.ORDER_COST_USD
    , ot.ORDER_TOTAL_USD
    , ot.SHIPPING_COST_USD
    , ot.STATUS
    , timestampdifF(hour,ot.Received_at_Timestamp,ot.Delivered_at_Timestamp) as Lag_respect_received_order_hours
    , timestampdifF(hour,ot.Estimated_delivery_at_timestamp,ot.Delivered_at_Timestamp) as Lag_respect_estimated_delivery_order_hours
    , ot.Load_Timestamp
    , case 
        when e.PAGE_URL is null then 'Tienda física'
        else 'Página web'
      end as Metodo_compra
from stg_order_tracking ot

left join stg_events e 
on ot.NK_orders=e.NK_orders

left join dim_shipping_address a 
on ot.NK_address=a.NK_address

left join intermediate_order o 
on ot.NK_orders=o.NK_orders

left join dim_promos p 
on ot.Promotion_Name=p.Promotion_Name

left join dim_customer c 
on ot.NK_users=c.NK_customer

left join intermediate_session s 
on e.DD_session=s.DD_session

{% if is_incremental() %}

 where ot.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}

