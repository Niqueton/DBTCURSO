

with boi as (
    select NK_products,number_of_units,Load_Timestamp from {{ ref('base_order_items') }}
),

aux1 as (
    select 
        NK_products,
        round(1.2*sum(number_of_units),0) as Security_Stock
    from boi
    where timestampadd(month,1,Load_Timestamp)>current_timestamp()
    group by NK_products
)

select * from aux1