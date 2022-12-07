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
        Valid_from as Last_time_updated_at
from dim_products_historica
where Valid_to is null

{% if is_incremental() %}

  where Valid_from >= (select max(Last_time_updated_at) from {{ this }})

{% endif %}