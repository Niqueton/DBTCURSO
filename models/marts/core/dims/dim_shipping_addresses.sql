{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_SHIPPING_ADDRESS',
    tags=['GOLD','INCREMENTAL','DIMS']
    ) 
    }}

with stg_addresses as (
    select * from {{ ref('stg_addresses') }}
), anexo as (
    select 
   
    0,
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    'No aplica',
    null 
)



select 
    ID_DIM_SHIPPING_ADDRESS,
    ADDRESS,
    NK_address,
    STATE,
    COUNTRY,
    City,
    ZIPCODE::varchar as Zipcode,
    hour_zone as time_zone,
    Load_Timestamp
from stg_addresses a

{% if is_incremental() %}

where Load_Timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}

union 

select * from anexo
   