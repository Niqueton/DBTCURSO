{{ config(
    materialized='incremental',
    unique_key = 'ID_ORDER_TRACKING'
    ) 
    }}

with base_orders1 as (
    select * from {{ ref('base_orders') }}
)

    select
        ID_ORDER_TRACKING,
        ADDRESS_ID,
        Received_at_Timestamp,
        Delivered_at_Timestamp,
        Estimated_Delivery_at_Timestamp,
        ORDER_COST_IN_DOLLARS,
        NK_orders,
        ORDER_TOTAL_IN_DOLLARS,
        PROMO_ID,
        SHIPPING_COST_IN_DOLLARS,
        SHIPPING_SERVICE,
        STATUS,
        TRACKING_ID,
        USER_ID,
        Load_Timestamp,
        timestampdifF(hour,Received_at_Timestamp,Delivered_at_Timestamp) as Lag_respect_received_order,
        timestampdifF(hour,Estimated_delivery_at_timestamp,Delivered_at_Timestamp) as Lag_respect_estimated_delivery_order
    from base_orders1

{% if is_incremental() %}

  where Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}