

{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_products',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}

with base_products1 as (
    select * from {{ source('src_sql_server', 'products') }}
)

    select 

    	product_ID as NK_products,
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
        to_time(_fivetran_synced) as Load_Time,
        _fivetran_synced as Load_Timestamp

	from base_products1
	where _fivetran_deleted is null

{% if is_incremental() %}

  and _fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}
