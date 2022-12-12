

with base_promos1 as (
    select * from {{ ref('stg_promos_snapshot') }}
)

select 
        md5(concat(DBT_VALID_FROM,Promotion_Name)) as ID_DIM_promos,
        (rank() over(partition by Promotion_Name order by DBT_VALID_FROM desc)) as Version,
        Promotion_Name,
        Order_discount_USD,
        DBT_VALID_FROM AS VALID_FROM,
        DBT_VALID_TO as VALID_TO
from base_promos1
where status='active'