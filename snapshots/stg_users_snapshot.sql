
{% snapshot stg_users_snapshot %}

{{
        config(
          unique_key='NK_users',
          strategy='timestamp',
          updated_at='Load_Date'
        )
    }}


with base_users as (
    select * from {{ ref('base_users') }}
)

    SELECT 
    	ID_DIM_users,
    	NK_users ,
		ADDRESS_ID ,
		LAST_NAME ,
		Create_Date ,
		Create_Time,
		PHONE_NUMBER ,
		FIRST_NAME ,
		EMAIL ,
        Load_Date,
        Load_Time 
    FROM 
    base_users

{% endsnapshot %}