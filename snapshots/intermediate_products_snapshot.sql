{% snapshot intermediate_products_snapshot %}

    {{
        config(
          strategy='check',
          unique_key='NK_products',
          check_cols=['Product_base_Price', 'Description','Product_Name'],
          invalidate_hard_deletes=True
        )
    }}





with ProductDesc as (
    select * from {{ source('src_others', 'productdesc') }}
),
base_products1 as (
    select * from {{ ref('base_products') }}
),


stg_products1 as (
    select 
        p.ID_DIM_products,
    	p.NK_products ,
		p.Product_base_Price ,
		p.Product_Name,
        p.Price_Range,
        pd.Description,
        p.Load_Date,
        p.Load_Time
    from base_products1 as p

    left join ProductDesc as pd
    on p.product_name=pd.product_name

)

select * from stg_products1

{% endsnapshot %}