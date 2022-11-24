{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_CUSTOMER',
    ) 
    }}


with stg_users_snapshot as (
    select * from {{ ref('stg_users_snapshot') }}
),

stg_addresses as (
    select * from {{ ref('stg_addresses') }}
)

select 
    u.ID_DIM_USERS as ID_DIM_CUSTOMER ,
	u.NK_USERS as NK_customer,
	(rank()over(partition by u.NK_users order by u.DBT_VALID_FROM desc)) as Version,
	concat(a.ADDRESS,' ,',a.city,' ,',a.Zipcode,',',a.STATE) as Main_Address,
	concat(u.FIRST_NAME,' ',u.LAST_NAME) as Complete_Name,
	u.PHONE_NUMBER ,
	u.EMAIL ,
	u.LOAD_DATE ,
	u.LOAD_TIME ,
	u.DBT_VALID_FROM as VALID_FROM,
	u.DBT_VALID_TO as VALID_TO
from stg_users_sanpshot u
left join stg_addresses a
on u.ADDRESS_ID=a.NK_address


{% if is_incremental() %}

  where u.DBT_VALID_FROM > (select max(VALID_FROM) from {{ this }})
  or (u.DBT_VALID_TO is not null and u-DBT_VALID_TO>= (select max(valid_from) from {{ this }})

{% endif %}