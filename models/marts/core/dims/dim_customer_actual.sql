{{ config(
    materialized='incremental',
    unique_key = 'NK_customer'
    ) 
    }}


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
	VALID_FROM,
    ID_LOAD_DATE

from dim_customer_historica
where VALID_TO is null

{% if is_incremental() %}

  where NK_customer in  (select NK_customer from dim_customer_historica where ID_LOAD_DATE>
  (select max(ID_LOAD_DATE) from {{ this }}))
  

{% endif %}
