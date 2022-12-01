
{{ config(
    materialized='incremental',
    unique_key = 'ID_ITEM_SALES',
    tags=['SILVER','Incremental']
    ) 
    }}

with base_order_items1 as (
    select * from {{ source('src_sql_server', 'order_items') }}
),
base_order_items2 as (
    select 
    md5(concat(PRODUCT_ID,ORDER_ID)) as ID_ITEM_SALES,
    PRODUCT_ID as NK_products,
    ORDER_ID as NK_orders,
    QUANTITY as NUMBER_OF_UNITS,
    _fivetran_synced as Load_Timestamp,
    _fivetran_deleted

    from base_order_items1

),

{{ fuera_deletes('base_order_items2','NK_products')}}

{% if is_incremental() %}

  where Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}

