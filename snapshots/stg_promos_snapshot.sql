{% snapshot stg_promos_snapshot %}

{{
        config(
          unique_key='Promotion_Name',
          strategy='timestamp',
          updated_at='Load_Date',
          tags=['SILVER','SNAPSHOT','INCREMENTAL']
        )
    }}



with base_promos1 as (
    select * from {{ source('src_sql_server', 'promos') }}
),

base_promos2 as (
    select 
        ID_DIM_promos,
        promo_id as Promotion_Name,
        discount as Order_discount_USD,
        status,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time,
        _fivetran_deleted
    from base_promos1


),

{{ fuera_deletes('base_promos2','Promotion_Name')}}


{% endsnapshot %}