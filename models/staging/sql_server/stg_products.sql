with base_products as (
    select * from {{ ref('base_products') }}
),
base_order_items as (
    select * from {{ ref('base_order_items') }}
),
base_orders as (
    select * from {{ ref('base_orders') }}
),
stg_products1 as (
    select 
    sum(o.NUMBER_OF_UNITS) as Item_sold_last_month,p.Product_name
    from base_order_items as o
    left join base_products as p
    on o.PRODUCT_ID=p.NK_product
    left join base_orders as ord
    on o.ORDER_ID=ord.NK_orders
    group by p.Name
    having dateadd(Month,1,ord.Received_at_Date)>=current_date()
),
stg_products2 as (
    select 
    	p.ID_DIM_products,
    	p.NK_product ,
		p.Product_base_Price ,
		p.Product_Name,
        p.INVENTORY,
		p.Price_Range,
        p.Load_Date,
        p.Load_Time 
    from base_products as p
    left join stg_products1 as s
    on p.Product_Name=s.Product_Name
)