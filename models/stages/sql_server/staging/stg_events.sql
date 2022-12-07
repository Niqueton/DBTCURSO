{{ config(
    materialized='incremental',
    unique_key = 'ID_WEB_INTERACTION',
    tags=['SILVER','INCREMENTAL']
    ) 
    }}

with base_events1 as (
    select * from 
    {{ source('src_sql_server', 'events') }}
),

sesion as (
    select session_id from base_events1 where Order_id is not null
)

    select 
        ID_WEB_INTERACTION,
        CREATED_AT as Produced_at_Timestamp,
        to_date(CREATED_AT) as Produced_at_Date,
        to_time(CREATED_AT) as Produced_at_Time ,
        EVENT_ID as NK_events,
        EVENT_TYPE,
        {{ cambio_valor('ORDER_ID',"''","null") }} as NK_orders,
        PAGE_URL,
        {{ cambio_valor('PRODUCT_ID',"''","null") }} as NK_products,
        e.SESSION_ID as DD_Session,
        USER_ID as NK_users,
        _fivetran_synced as Load_Timestamp,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time,
        case
            when s.session_id is null then 'No terminó en compra'
            else 'Terminó en compra'
        end as Indicador_compra

    from base_events1 e

    left join sesion s
    on e.Session_id=s.session_id

    where _fivetran_deleted is null


{% if is_incremental() %}

 and _fivetran_synced > (select max(Load_Timestamp) from {{ this }})

{% endif %}