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
		case 
			when PRICE<35 then 'low range'
			when PRICE<55 then 'low-mid range'
			when price<75 then 'high-mid range'
			else 'high range'
		end as Price_Range,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time 
	from base_products1
	where _fivetran_deleted is null
)

select * from base_products2