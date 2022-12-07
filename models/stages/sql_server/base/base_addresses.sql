
{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_SHIPPING_ADDRESS',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}


with base_addresses1 as (
    select * from {{ source('src_sql_server', 'addresses') }}
)

    select 
        a.ID_SHIPPING_ADDRESS as ID_DIM_SHIPPING_ADDRESS,
        a.ADDRESS,
        a.ADDRESS_ID as NK_address,
        a.STATE,
        a.COUNTRY,
        a.ZIPCODE,
        a._fivetran_synced as Load_Timestamp

    from base_addresses1 a
    where a._fivetran_deleted is null


{% if is_incremental() %}

  and a._fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}