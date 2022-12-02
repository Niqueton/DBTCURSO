{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_SHIPPING_ADDRESS',
    tags=['GOLD','INCREMENTAL','DIMS']
    ) 
    }}

with stg_addresses as (
    select * from {{ ref('stg_addresses') }}
), 

anexo as (
    select  0,'No aplica','No aplica','No aplica','No aplica','No aplica','No aplica','No aplica', 0, 0
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
    {{ fecha_id('to_date(Load_Timestamp)') }} as ID_LOAD_DATE,
    {{ time_id('to_time(Load_Timestamp)') }} as ID_LOAD_TIME
from stg_addresses

{% if is_incremental() %}

where {{ fecha_id('to_date(Load_Timestamp)')}} > (select max(ID_LOAD_DATE) from {{ this }})

{% endif %}

union 

select * from anexo
   