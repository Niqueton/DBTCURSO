with base_products1 as (
    select * from {{ source('src_sql_server', 'products') }}
)
,
base_products2 as (
    select 
    	ID_DIM_products,
    	product_ID as NK_product ,
		PRICE as Product_base_Price ,
		NAME as Product_Name,
        INVENTORY,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time 
	from base_users1
	where _fivetran_deleted=False
)

select * from base_products2