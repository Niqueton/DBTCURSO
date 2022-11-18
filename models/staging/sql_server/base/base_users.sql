with us as (
    select * from {{ source('src_sql_server', 'users') }}
)
,
useri as (
    select 
    	ROW_NUMBER() over(order by USER_ID),
    	USER_ID ,
		UPDATED_AT ,
		ADDRESS_ID ,
		LAST_NAME ,
		CREATED_AT ,
		PHONE_NUMBER ,
		FIRST_NAME ,
		EMAIL ,
		_FIVETRAN_DELETED,
		_FIVETRAN_SYNCED as Load_Timestamp 
	from us
)

select * from useri