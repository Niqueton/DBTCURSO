with dim_customer_historica as (
    select * from {{ ref('dim_customer_historica') }}
)

select distinct
    ID_DIM_CUSTOMER ,
	NK_customer,
	Version,
	Main_Address,
	Complete_Name,
	PHONE_NUMBER ,
	EMAIL ,
	LOAD_DATE ,
	LOAD_TIME ,
	VALID_FROM

from dim_customer_historica
where VALID_TO is null

union

select 
	 0
	,'No aplica'
	,0
	,'No aplica'
	,'No aplica'
	,'No aplica'
	,'No aplica'
	,to_date('0001-01-01')
	,to_time('00:00:00')
	,to_date('0001-01-01')

