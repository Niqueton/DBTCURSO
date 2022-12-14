{{ config(
    materialized='incremental',
    unique_key = ['NK_products','Version'],
    tags= ['INCREMENTAL']
    ) 
    }}

with intermediate_products_snapshot as (
    select * from {{ ref('intermediate_products_snapshot') }}
)
select 
        md5(concat(NK_products,DBT_VALID_FROM)) as ID_DIM_products,
        rank()over(partition by NK_products order by DBT_VALID_FROM) as Version,
      	NK_products ,
	    	Product_base_Price,
	    	Product_Name,
        Price_Range,
        Description,
	      {{ fecha_id('LOAD_DATE') }} as ID_LOAD_DATE ,
	      {{ time_id('LOAD_TIME') }} as ID_LOAD_TIME,
        DBT_VALID_FROM as Valid_from,
        DBT_VALID_TO as Valid_to


from intermediate_products_snapshot



{% if is_incremental() %}

  where NK_products in  (select NK_products from intermediate_products_snapshot where {{ fecha_id('LOAD_DATE') }}>
  (select max(ID_LOAD_DATE) from {{ this }}))

{% endif %}

union 

select 'No aplica',0,'No aplica',0,'No aplica','No aplica','No aplica',0,0,to_date('2000-03-01'),null