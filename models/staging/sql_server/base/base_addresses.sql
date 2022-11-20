with base_addresses1 as (
    select * from {{ source('src_sql_server', 'addresses') }}
),
base_addresses2 as (
    select 
        ID_SHIPPING_ADDRESS,
        ADDRESS,
        ADDRESS_ID as NK_address,
        COUNTRY,
        ZIPCODE,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time
    from base_addresses1
    where _fivetran_deleted is null
)
select * from base_addresses2