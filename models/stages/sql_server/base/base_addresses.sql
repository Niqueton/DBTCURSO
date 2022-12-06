
{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_SHIPPING_ADDRESS',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}


with base_addresses1 as (
    select * from {{ source('src_sql_server', 'addresses') }}
),

base_addresses2 as (
    select 
        a.ID_SHIPPING_ADDRESS as ID_DIM_SHIPPING_ADDRESS,
        a.ADDRESS,
        a.ADDRESS_ID as NK_address,
        a.STATE,
        a.COUNTRY,
        a.ZIPCODE,
        a._fivetran_synced as Load_Timestamp,
        a._fivetran_deleted
    from base_addresses1 a
),

{{ fuera_deletes('base_addresses2','NK_address')}}


{% if is_incremental() %}

  where a._fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}