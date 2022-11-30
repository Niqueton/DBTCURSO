{% snapshot stg_products_snapshot %}

{{
        config(
          unique_key='NK_product',
          strategy='timestamp',
          updated_at='Load_Date'
        )
    }}





with ProductDesc as (
    select * from {{ source('src_others', 'productdesc') }}
),
base_products1 as (
    select * from {{ ref('base_products') }}
),

stg_security_stock as (
    select * from {{ ref('stg_security_stock') }}
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
        s.Security_Stock,
        p.Load_Date,
        p.Load_Time
    from base_products1 as p
    left join ProductDesc as pd
    on p.product_name=pd.product_name
    left join stg_security_stock s 
    on p.NK_product=s.PRODUCT_ID
)

select * from stg_products1

{% endsnapshot %}