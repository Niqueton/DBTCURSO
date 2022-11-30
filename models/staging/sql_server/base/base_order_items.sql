

with base_order_items1 as (
    select * from {{ source('src_sql_server', 'order_items') }}
),
base_order_items2 as (
    select 
    
    PRODUCT_ID as NK_products,
    ORDER_ID as NK_orders,
    QUANTITY as NUMBER_OF_UNITS,
    _fivetran_synced as Load_Timestamp,
    _fivetran_deleted

    from base_order_items1

),

{{ fuera_deletes('base_order_items2','NK_products')}}


