{{ config(
    materialized='incremental',
    unique_key = 'ID_ORDER_TRACKING',
    tags=['SILVER','Incremental']
    ) 
    }}


with base_orders1 as (
    select * from {{ source('src_sql_server', 'orders') }}
),

auxi as (
    select
        ID_ORDER_TRACKING,
        ADDRESS_ID as NK_address,
        CREATED_AT as Received_at_Timestamp,
        DELIVERED_AT as Delivered_at_Timestamp,
        ESTIMATED_DELIVERY_AT as Estimated_Delivery_at_Timestamp,
        ORDER_COST as ORDER_COST_IN_DOLLARS,
        ORDER_ID as NK_orders,
        ORDER_TOTAL as ORDER_TOTAL_IN_DOLLARS,
        PROMO_ID as Promotion_Name,
        SHIPPING_COST as SHIPPING_COST_IN_DOLLARS,
        SHIPPING_SERVICE,
        STATUS,
        TRACKING_ID as DD_tracking,
        USER_ID as NK_users,
        _fivetran_synced as Load_Timestamp,
        _fivetran_deleted
        
from base_orders1
),

{{ fuera_deletes('auxi','NK_orders')}}

{% if is_incremental() %}

  where _fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}












  
