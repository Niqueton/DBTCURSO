
{% snapshot base_users_snapshot %}

{{
        config(
          unique_key='NK_users',
          strategy='timestamp',
          updated_at='Load_Date',
          tags= ['SILVER','SNAPSHOT']
        )
    }}

with base_users1 as (
    select * from {{ source('src_sql_server', 'users') }}
)
,
base_users2 as (
  select 
    ID_DIM_users,
    User_ID as NK_users ,
		to_date(UPDATED_AT) as Update_Date ,
		to_time(UPDATED_AT) as Update_Time,
		ADDRESS_ID as NK_address,
		LAST_NAME ,
		to_date(created_AT) as Create_Date ,
		to_time(created_AT) as Create_Time,
		PHONE_NUMBER ,
		FIRST_NAME ,
		EMAIL ,
    to_date(_fivetran_synced) as Load_Date,
    to_time(_fivetran_synced) as Load_Time 
	from base_users1
	where _fivetran_deleted is null
)

select * from base_users2


{% endsnapshot %}