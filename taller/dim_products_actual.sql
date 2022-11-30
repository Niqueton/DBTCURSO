with dim_products_historica as (
    select * from {{ ref('dim_products_historica') }}
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
        Valid_from as Last_time_updated_at
from dim_products_historica
where Valid_to is null