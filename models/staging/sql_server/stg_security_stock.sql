
with boi as (
    select Product_id,number_of_units,Load_date from {{ ref('base_order_items') }}
),

aux1 as (
    select 
        Product_id,
        round(1.2*sum(number_of_units),0) as Security_Stock
    from boi
    where timestampadd(month,1,Load_date)>current_timestamp()
    group by Product_id
)

select * from aux1