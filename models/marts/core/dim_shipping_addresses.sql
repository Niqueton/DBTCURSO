{{ config(
    post_hook="{{ nulos_a_desconocidos('{{ this }}',city) }}"
) }}


with stg_addresses as (
    select * from {{ ref('stg_addresses') }}
)


select 
    a.ID_SHIPPING_ADDRESS,
    a.ADDRESS,
    a.NK_address,
    a.STATE,
    a.COUNTRY,
    a.ZIPCODE,
    a.city,
    a.hour_zone as time_zone,
    a.Load_Date,
    a.Load_Time
from stg_addresses a

