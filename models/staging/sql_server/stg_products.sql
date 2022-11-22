with ProductDesc as (
    select * from {{ source('src_others', 'productdesc') }}
),
base_products1 as (
    select * from {{ ref('base_products') }}
),

stg_products1 as (
    select 
        p.ID_DIM_products,
    	p.NK_product ,
		p.Product_base_Price ,
		p.Product_Name,
        p.INVENTORY as Stock,
        p.Price_Range,
        pd.Description,
        p.Load_Date,
        p.Load_Time
    from base_products1 as p
    left join ProductDesc as pd
    on p.Product_name=pd.Product_name
)

select * from stg_products1

