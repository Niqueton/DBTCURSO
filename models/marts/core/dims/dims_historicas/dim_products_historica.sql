{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_products',
    tags= ['INCREMENTAL']
    ) 
    }}

with intermediate_products_snapshot as (
    select * from {{ ref('intermediate_products_snapshot') }}
)
select 
        ID_DIM_products,
        rank()over(partition by NK_products order by DBT_VALID_FROM desc) as Version,
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

  where {{ fecha_id('Load_date') }} >= (select max(ID_Load_Date) from {{ this }})

{% endif %}

union 

select 0,0,'No aplica',0,'No aplica','No aplica','No aplica',0,0,to_date('2000-03-01'),null