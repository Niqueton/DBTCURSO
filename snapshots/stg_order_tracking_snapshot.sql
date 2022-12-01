{% snapshot stg_order_tracking_snapshot %}

{{
        config(
          unique_key='NK_orders',
          strategy='timestamp',
          updated_at='Load_Timestamp',
          tags=['SILVER','SNAPSHOT','INCREMENTAL']
        )
    }}


with base_orders1 as (
    select * from {{ ref('base_orders') }}
),
base_order_items as (
    select * from {{ ref('base_order_items') }}
)

    select
        o.ID_ORDER_TRACKING,
        o.NK_address,
        o.Received_at_Timestamp,
        o.Delivered_at_Timestamp,
        o.Estimated_Delivery_at_Timestamp,
        o.ORDER_COST_IN_DOLLARS,
        o.NK_orders,
        o.ORDER_TOTAL_IN_DOLLARS,
        o.Promotion_name,
        o.SHIPPING_COST_IN_DOLLARS,
        o.SHIPPING_SERVICE,
        o.STATUS,
        o.DD_tracking,
        o.NK_users,
        case
            when o.ORDER_TOTAL_IN_DOLLARS < 25 then '0-25 USD'
            when o.ORDER_TOTAL_IN_DOLLARS < 50 then '25-50 USD'
            when o.ORDER_TOTAL_IN_DOLLARS < 75 then '50-75 USD'
            when o.ORDER_TOTAL_IN_DOLLARS < 100 then '75-100 USD'
            when o.ORDER_TOTAL_IN_DOLLARS < 125 then '100-125 USD'
            when o.ORDER_TOTAL_IN_DOLLARS < 150 then '125-150 USD'
            else '> 150 USD'
        end as RANGE_ORDER_TOTAL_USD,
        count(oi.NK_products) as Number_of_different_items,
        sum(oi.NUMBER_OF_UNITS) as Total_number_of_items,
        o.Load_Timestamp,
        timestampdifF(hour,o.Received_at_Timestamp,o.Delivered_at_Timestamp) as Lag_respect_received_order,
        timestampdifF(hour,o.Estimated_delivery_at_timestamp,o.Delivered_at_Timestamp) as Lag_respect_estimated_delivery_order
    from base_orders1 o 
    left join base_order_items oi 
    on o.NK_orders=oi.NK_orders
    group by 
        o.ID_ORDER_TRACKING,
        o.NK_address,
        o.Received_at_Timestamp,
        o.Delivered_at_Timestamp,
        o.Estimated_Delivery_at_Timestamp,
        o.ORDER_COST_IN_DOLLARS,
        o.NK_orders,
        o.ORDER_TOTAL_IN_DOLLARS,
        o.Promotion_Name,
        o.SHIPPING_COST_IN_DOLLARS,
        o.SHIPPING_SERVICE,
        o.STATUS,
        o.DD_tracking,
        o.NK_users,
        o.Load_Timestamp,
        timestampdifF(hour,o.Received_at_Timestamp,o.Delivered_at_Timestamp),
        timestampdifF(hour,o.Estimated_delivery_at_timestamp,o.Delivered_at_Timestamp)

{% endsnapshot %}