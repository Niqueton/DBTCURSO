select 1 from {{ ref('stg_order_tracking') }}
where Number_of_different_items>Total_number_of_items