{{
    config(
        materialized='incremental',
        unique_key='ID_WEB_INTERACTION',
        on_schema_change='fail'
    )
}}



with base_events1 as (
    select * from 
    {{ source('src_sql_server', 'events') }}
),
base_events2 as (
    select 
        ID_WEB_INTERACTION,
        to_date(CREATED_AT) as Produced_at_Date,
        to_time(CREATED_AT) as Produced_at_Time ,
        EVENT_ID,
        EVENT_TYPE,
        ORDER_ID,
        PAGE_URL,
        PRODUCT_ID,
        SESSION_ID,
        USER_ID,
        _fivetran_synced as Load_Timestamp
    from base_events1
    where _fivetran_deleted is null
)

select * from base_events2

{% if is_incremental() %}

  where _fivetran_synced > (select max(Load_Timestamp) from {{ this }})
  
{% endif %}