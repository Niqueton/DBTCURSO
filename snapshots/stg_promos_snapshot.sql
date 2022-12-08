{% snapshot stg_promos_snapshot %}

{{
        config(
          unique_key='promo_id',
          strategy='timestamp',
          updated_at='Load_Date',
          tags=['SILVER','INCREMENTAL'],
          invalidate_hard_deletes=True
        )
    }}



with base_promos1 as (
    select * from {{ source('src_sql_server', 'promos') }}
)


    select 

        promo_id as Promotion_Name,
        discount as Order_discount_USD,
        status,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time

    from base_promos1
    where _fivetran_deleted is null



{% endsnapshot %}