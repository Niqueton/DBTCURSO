{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_products',
    tags= ['INCREMENTAL']
    ) 
    }}

with dim_products_historica as (
    select * from {{ ref('dim_products_historica') }}
)
select 
        ID_DIM_products,
      	NK_products ,
	      Product_base_Price ,
	      Product_Name,
        Price_Range,
        Description,
        Valid_from as Last_time_updated_at,
        ID_LOAD_DATE
from dim_products_historica


{% if is_incremental() %}

  where ID_LOAD_DATE> (select max(ID_LOAD_DATE) from {{ this }})

{% endif %}