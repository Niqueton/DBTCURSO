with base_promos1 as (
    select * from {{ ref('base_promos') }}
)

select 
        ID_DIM_promos,
        Promotion_Name,
        Order_discount_in_Dollars,
        status,
        Load_Date,
        Load_Time
from base_promos1