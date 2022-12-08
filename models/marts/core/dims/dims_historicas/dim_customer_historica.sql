 {{ config(
    materialized='incremental',
    unique_key = ['NK_customer','Version']
    ) 
    }}



with u as (
    select * from {{ ref('base_users_snapshot') }}
),

stg_addresses as (
    select * from {{ ref('stg_addresses') }}
)

select 
    md5(concat(NK_USERS,{{ fecha_id('u.LOAD_DATE') }}::varchar)) as ID_DIM_CUSTOMER ,
	u.NK_USERS as NK_customer,
	(rank()over(partition by u.NK_users order by u.DBT_VALID_FROM desc)) as Version,
	concat(a.ADDRESS,' ,',a.city,' ,',a.Zipcode,',',a.STATE) as Main_Address,
	concat(u.FIRST_NAME,' ',u.LAST_NAME) as Complete_Name,
	u.PHONE_NUMBER ,
	u.EMAIL ,
	{{ fecha_id('u.LOAD_DATE') }} as ID_LOAD_DATE,
	{{ time_id('u.LOAD_TIME') }} as ID_LOAD_TIME,
	u.DBT_VALID_FROM as VALID_FROM,
	u.DBT_VALID_TO as VALID_TO

from u

left join stg_addresses as a
on u.NK_address=a.NK_address

{% if is_incremental() %}

  where NK_users in  (select NK_users from u where {{ fecha_id('u.LOAD_DATE') }}>
  (select max(ID_LOAD_DATE) from {{ this }}))
  

{% endif %}

union 

select 
	 'No aplica'
	,'No aplica'
	,0
	,'No aplica'
	,'No aplica'
	,'No aplica'
	,'No aplica'
	,0
	,0
	,to_date('0001-01-01')
	,null
