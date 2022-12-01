

with base_events1 as (
    select * from 
    {{ source('src_sql_server', 'events') }}
),

auxi as (
    select 
        ID_WEB_INTERACTION,
        to_date(CREATED_AT) as Produced_at_Date,
        to_time(CREATED_AT) as Produced_at_Time ,
        EVENT_ID as NK_events,
        EVENT_TYPE,
        {{ cambio_substring('ORDER_ID',"''","null") }} as NK_orders,
        PAGE_URL,
        {{ cambio_substring('PRODUCT_ID',"''","null") }} as NK_products,
        SESSION_ID as DD_Session,
        USER_ID as NK_users,
        _fivetran_synced as Load_Timestamp,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time,
        _fivetran_deleted
    from base_events1
),

{{ fuera_deletes('auxi','NK_events')}}

