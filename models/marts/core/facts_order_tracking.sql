{{ config(
    materialized='incremental',
    unique_key = 'ID_ORDER_TRACKING'
    ) 
    }}


with stg_order_tracking as (
    select * from {{ ref('stg_order_tracking_snapshot') }}
),

dim_shipping_address as (
    select * from {{ ref('dim_shipping_addresses') }}
),

intermediate_order as (
    select * from {{ ref('intermediate_order') }}
),

dim_promos as (
    select * from {{ ref('dim_promos') }}
),

dim_customer as (
    select * from {{ ref('dim_customer_historica') }}
),

dim_date_received as (
    select * from {{ ref('dim_date') }}
),

dim_date_delivered as (
    select * from {{ ref('dim_date') }}
),

dim_date_estimated_delivery as (
    select * from {{ ref('dim_date') }}
),

base_events as (
    select * from {{ ref('base_events') }}
)



select 
     ot.ID_ORDER_TRACKING
    ,  c.ID_DIM_CUSTOMER
    , p.ID_DIM_promos
    , a.ID_SHIPPING_ADDRESS
    , ot.NK_orders as DD_order_id
    , ot.TRACKING_ID as DD_Tracking_id
    , d1.id_date as Received_at_Date
    , d2.id_date as Delivered_at_Date
    , d3.id_date as Estimated_delivery_at_Date
    , ot.ORDER_COST_IN_DOLLARS
    , ot.ORDER_TOTAL_IN_DOLLARS
    , ot.SHIPPING_COST_IN_DOLLARS
    , ot.STATUS
    , ot.Lag_respect_received_order
    , ot.Lag_respect_estimated_delivery_order
    , ot.Load_Timestamp
    , case 
        when e.PAGE_URL is null then 'Tienda física'
        else e.PAGE_URL
      end as Place_of_purchase
from stg_order_tracking ot

left join base_events e 
on ot.NK_orders=e.ORDER_ID

left join dim_shipping_address a 
on ot.ADDRESS_ID=a.NK_address

left join intermediate_order o 
on ot.NK_orders=o.NK_orders

left join dim_promos p 
on ot.PROMO_ID=p.Promotion_Name

left join dim_customer c 
on ot.USER_ID=c.NK_customer

left join dim_date_received d1 
on to_date(ot.Received_at_Timestamp)=d1.date_day

left join dim_date_delivered d2
on to_date(ot.Delivered_at_Timestamp)=d2.date_day

left join dim_date_estimated_delivery d3
on to_date(ot.Estimated_Delivery_at_Timestamp)=d3.date_day

where ot.DBT_VALID_TO is null

{% if is_incremental() %}

  and ot.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}

