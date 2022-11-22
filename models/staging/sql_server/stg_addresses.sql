with base_addresses as (
    select * from {{ ref('base_addresses') }}
),
husos_horarios as(
    select * from {{ ref('husos_horarios') }}
),

stg_addresses1 as (
    select 
        ID_SHIPPING_ADDRESS,
        ADDRESS,
        NK_address,
        a.STATE,
        COUNTRY,
        ZIPCODE,
        h.hour_zone,
        Load_Date,
        Load_Time
        
    from base_addresses as a
    left join husos_horarios as h
    on a.STATE=h.STATE
)

select * from stg_addresses1