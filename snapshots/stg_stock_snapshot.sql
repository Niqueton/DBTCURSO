{% snapshot stg_stock_snapshot %}

{{
        config(
          unique_key='ID_STOCK',
          strategy='timestamp',
          updated_at='Load_Date',
          tags= ['SILVER','INCREMENTAL']
        )
    }}



with base_products as (
    select * from {{ ref('base_products') }}
)

select 

  md5(concat(NK_products,year(Load_Date),month(Load_Date))) as ID_STOCK,
  NK_products,
  INVENTORY as Stock,
  year(Load_date)*100+month(Load_date) as id_anio_mes,
  Load_Date

from base_products


{% endsnapshot %}

