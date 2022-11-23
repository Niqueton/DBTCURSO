


with base_orders1 as (
    select * from {{ source('src_sql_server', 'orders') }}
)


    select
        ID_ORDER_TRACKING,
        ADDRESS_ID,
        CREATED_AT as Received_at_Timestamp,
        DELIVERED_AT as Delivered_at_Timestamp,
        ESTIMATED_DELIVERY_AT as Estimated_Delivery_at_Timestamp,
        ORDER_COST as ORDER_COST_IN_DOLLARS,
        ORDER_ID as NK_orders,
        ORDER_TOTAL as ORDER_TOTAL_IN_DOLLARS,
        PROMO_ID,
        SHIPPING_COST as SHIPPING_COST_IN_DOLLARS,
        SHIPPING_SERVICE,
        STATUS,
        TRACKING_ID,
        USER_ID,
        _fivetran_synced as Load_Timestamp
        
from base_orders1
where _fivetran_deleted is null










  
