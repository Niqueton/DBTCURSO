
with stg_addresses as (
    select * from {{ ref('stg_addresses') }}
)


select 
    ID_SHIPPING_ADDRESS,
    ADDRESS,
    NK_address,
    STATE,
    COUNTRY,
    ZIPCODE::varchar as Zipcode,
    hour_zone as time_zone,
    Load_Date,
    Load_Time
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
    current_date(),
    current_time()  
   