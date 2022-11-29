{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_CUSTOMER',
    ) 
    }}

with stg_products_snapshot as (
    select * from {{ ref('stg_products_snapshot') }}
)
select 
        ID_DIM_products,
      	NK_product ,
	    	Product_base_Price ,
	    	Product_Name,
        Price_Range,
        Description,
        Security_Stock,
        Load_Date,
        Load_Time,
        DBT_VALID_FROM as Valid_from,
        DBT_VALID_TO as Valid_to
from stg_products_snapshot







{% if is_incremental() %}

  where Load_date >= (select max(Load_Date) from {{ this }})

{% endif %}