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



