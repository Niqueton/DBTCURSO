with base_addresses1 as (
    select * from {{ source('src_sql_server', 'addresses') }}
),


base_addresses2 as (
    select 
        a.ID_SHIPPING_ADDRESS,
        a.ADDRESS,
        a.ADDRESS_ID as NK_address,
        a.STATE,
        a.COUNTRY,
        a.ZIPCODE,
        to_date(a._fivetran_synced) as Load_Date,
        to_time(a._fivetran_synced) as Load_Time
    from base_addresses1 a
    where _fivetran_deleted is null
)

select * from base_addresses2