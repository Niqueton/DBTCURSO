
{{ config(
    materialized='incremental',
    unique_key = 'NK_orders',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}


with base_orders1 as (
    select * from {{ source('src_sql_server', 'orders') }}
)

    select
        ADDRESS_ID as NK_address,
        CREATED_AT as Received_at_Timestamp,
        DELIVERED_AT as Delivered_at_Timestamp,
        ESTIMATED_DELIVERY_AT as Estimated_Delivery_at_Timestamp,
        ORDER_COST as ORDER_COST_USD,
        ORDER_ID as NK_orders,
        ORDER_TOTAL as ORDER_TOTAL_USD,
        {{ cambio_valor('PROMO_ID',"''","null") }} as Promotion_Name,
        SHIPPING_COST as SHIPPING_COST_USD,
        SHIPPING_SERVICE,
        STATUS,
        TRACKING_ID as DD_tracking,
        USER_ID as NK_users,
        _fivetran_synced as Load_Timestamp

        
from base_orders1
where _fivetran_deleted is null

{% if is_incremental() %}

  and _fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}














  
