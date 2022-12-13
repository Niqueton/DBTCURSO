{{ config(
    materialized='incremental',
    unique_key=['NK_products','id_anio_mes_anterior'],
    tags=['SILVER','INCREMENTAL']
    ) 
    }}


with base_budget1 as (
    select * from {{ source('src_google_sheets', 'budget') }}
),
base_budget2 as (
    select 
        _row,
        MONTH as fecha,
        ADD_MONTHS( MONTH , 1 ) as fecha_plus,
        PRODUCT_ID as NK_products,
        QUANTITY as NUMBER_OF_UNITS_EXPECTED,
        _fivetran_synced

        from base_budget1
),
base_budget3 as (
    select 
        _row,
        (year(fecha)*100+month(fecha)) as id_anio_mes_anterior,
        (year(fecha_plus)*100+month(fecha_plus)) as id_anio_mes_prediccion,
        NK_products,
        NUMBER_OF_UNITS_EXPECTED,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time 
    from base_budget2
)

select * from base_budget3

{% if is_incremental() %}

  where Load_Date >= (select max(Load_date) from {{ this }})

{% endif %}