{{ config(
    post_hook=" update {{ this }} set city='Desconocido' where city is null "
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
    a.ZIPCODE::varchar as Zipcode,
    a.city,
    a.hour_zone as time_zone,
    a.Load_Date,
    a.Load_Time
from stg_addresses a

union

select 
   
    0,
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    current_date(),
    current_time()  
   