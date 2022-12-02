with base_promos1 as (
    select * from {{ ref('dim_promos_historica') }}
)

select 
        ID_DIM_promos,
        Promotion_Name,
        Order_discount_in_Dollars,
        VALID_FROM
from base_promos1
where VALID_TO is null