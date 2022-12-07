{{ config(
    materialized='incremental',
    unique_key = 'NK_orders',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}


with base_orders1 as (
    select * from {{ ref('base_orders') }}
),
base_order_items as (
    select * from {{ ref('base_order_items') }}
)

    select
        o.NK_address,
        o.Received_at_Timestamp,
        o.Delivered_at_Timestamp,
        o.Estimated_Delivery_at_Timestamp,
        o.ORDER_COST_USD,
        o.NK_orders,
        o.ORDER_TOTAL_USD,
        {{ cambio_valor('o.Promotion_name','null',"'No aplica'") }} as Promotion_Name,
        o.SHIPPING_COST_USD,
        o.SHIPPING_SERVICE,
        o.STATUS,
        o.DD_tracking,
        o.NK_users,
        case
            when o.ORDER_TOTAL_USD < 25 then '0-25 USD'
            when o.ORDER_TOTAL_USD < 50 then '25-50 USD'
            when o.ORDER_TOTAL_USD < 75 then '50-75 USD'
            when o.ORDER_TOTAL_USD < 100 then '75-100 USD'
            when o.ORDER_TOTAL_USD < 125 then '100-125 USD'
            when o.ORDER_TOTAL_USD < 150 then '125-150 USD'
            else '> 150 USD'
        end as RANGE_ORDER_TOTAL_USD,
        count(oi.NK_products) as Number_of_different_items,
        sum(oi.NUMBER_OF_UNITS) as Total_number_of_items,
        o.Load_Timestamp
    
    from base_orders1 o 
    left join base_order_items oi 
    on o.NK_orders=oi.NK_orders

    group by 

        o.NK_address,
        o.Received_at_Timestamp,
        o.Delivered_at_Timestamp,
        o.Estimated_Delivery_at_Timestamp,
        o.ORDER_COST_USD,
        o.NK_orders,
        o.ORDER_TOTAL_USD,
        o.Promotion_Name,
        o.SHIPPING_COST_USD,
        o.SHIPPING_SERVICE,
        o.STATUS,
        o.DD_tracking,
        o.NK_users,
        o.Load_Timestamp,
        timestampdifF(hour,o.Received_at_Timestamp,o.Delivered_at_Timestamp),
        timestampdifF(hour,o.Estimated_delivery_at_timestamp,o.Delivered_at_Timestamp)


{% if is_incremental() %}

  having o.Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}