with base_orders1 as (
    select * from {{ source('src_sql_server', 'orders') }}
),
base_orders2 as(
    select
        ID_ORDER_TRACKING,
        ADDRESS_ID,
        to_date(CREATED_AT) as Received_at_Date,
        to_time(CREATED_AT) as Received_at_Time ,
        to_date(DELIVERED_AT) as Delivered_at_Date,
        to_time(DELIVERED_AT) as Delivered_at_Time,
        to_date(ESTIMATED_DELIVERY_AT) as Estimated_Delivery_at_Date,
        to_time(ESTIMATED_DELIVERY_AT) as Estimated_Delivery_at_Time,
        ORDER_COST as ORDER_COST_IN_DOLLARS,
        ORDER_ID as NK_orders,
        ORDER_TOTAL as ORDER_TOTAL_IN_DOLLARS,
        PROMO_ID,
        SHIPPING_COST as SHIPPING_COST_IN_DOLLARS,
        SHIPPING_SERVICE,
        STATUS,
        TRACKING_ID,
        USER_ID,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time 
from base_orders1
where _fivetran_deleted is null
)

select * from base_orders2