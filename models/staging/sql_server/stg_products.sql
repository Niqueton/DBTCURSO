with ProductDesc as (
    select * from {{ source('src_others', 'productdesc') }}
),
base_products1 as (
    select * from {{ ref('base_products') }}
),
aux1 as (
    select 
        p.Product_Name,
        sum(o.quantity) as 
    from base_products1 p
    left join {{ ref('model_name') }}
    on p.PRODUCT_ID=o.PRODUCT_ID
    where timestampadd(month,1,o._fivetran_synced)>current_timestamp()
    group by p.Name;
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