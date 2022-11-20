with base_order_items1 as (
    select * from {{ source('src_sql_server', 'order_items') }}
),
base_order_items2 as (
    select 
    PRODUCT_ID,
    ORDER_ID,
    QUANTITY as NUMBER_OF_UNITS,
    to_date(_fivetran_synced) as Load_Date,
    to_time(_fivetran_synced) as Load_Time 
    from base_order_items1
    where _fivetran_deleted is null
)

select * from base_order_items2