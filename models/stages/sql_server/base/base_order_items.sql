
{{ config(
    materialized='incremental',
    unique_key = 'ID_ITEM_SALES',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}

with base_order_items1 as (
    select * from {{ source('src_sql_server', 'order_items') }}
)

    select 
    md5(concat(PRODUCT_ID,ORDER_ID)) as ID_ITEM_SALES,
    PRODUCT_ID as NK_products,
    ORDER_ID as NK_orders,
    QUANTITY as NUMBER_OF_UNITS,
    _fivetran_synced as Load_Timestamp,
    _fivetran_deleted

    from base_order_items1
    where _fivetran_deleted is null


{% if is_incremental() %}

  and _fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}

