{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_SHIPPING_ADDRESS',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}



with base_addresses as (
    select * from {{ ref('base_addresses') }}
),
husos_horarios as(
    select * from {{ ref('husos_horarios') }}
),
cityzipcode as (
    select * from {{ source('src_others', 'cityzipcode') }}
),

stg_addresses1 as (
    select 
        a.ID_DIM_SHIPPING_ADDRESS,
        a.ADDRESS,
        a.NK_address,
        a.STATE,
        a.COUNTRY,
        a.ZIPCODE,
        regexp_replace(c.city,'"') as city,
        h.hour_zone,
        a.Load_Timestamp
        
    from base_addresses as a

    left join husos_horarios as h
    on a.STATE=h.STATE
    
    left join cityzipcode c
    on a.ZIPCODE=c.ZIPCODE
    
)

select * from stg_addresses1

{% if is_incremental() %}

  where Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}