with facts_web_interaction as (
    select * from {{ ref('facts_web_interaction') }}
),

facts_item_sales as (
    select * from {{ ref('facts_item_sales') }}
),

dim_customer_historica as (
    select * from {{ ref('dim_customer_historica') }}
),

facts_order_tracking as (
    select * from {{ ref('facts_order_tracking') }}
)





select 

      c.ID_DIM_customer
    , c.Main_Address
    , c.EMAIL
    , c.Complete_Name
    , count(distinct it.DD_orders) as Numero_de_pedidos
    , sum(it.Quantity_sold) as Numero_total_de_items
    , count(distinct it.ID_DIM_products) as Numero_items_distintos
    , sum(o.ORDER_TOTAL_USD) as Gasto_total_de_cliente_USD
    , case 
            when count(distinct it.DD_orders)=0 then 0
            else (sum(o.ORDER_TOTAL_USD)/count(distinct it.DD_orders))
      end as Gasto_medio_pedido_USD

    , count(distinct w.DD_session) as Numero_sesiones_web
    , case 
            when count(distinct w.DD_session)=0 then 'No aplica'
            else concat((100*count(distinct it.DD_orders)/count(distinct w.DD_session))::varchar,'%')
      end as Ratio_de_finalizacon_en_compra
    , sum(it.Proportional_discount_USD) as Descuento_total_USD

from dim_customer_historica as c 

left join facts_item_sales as it 
on c.ID_DIM_Customer=it.ID_DIM_Customer

left join facts_web_interaction as w 
on c.ID_DIM_Customer=w.ID_DIM_Customer

left join facts_order_tracking o 
on c.ID_DIM_CUSTOMER=o.ID_DIM_CUSTOMER

group by 
      c.ID_DIM_customer
    , c.Main_Address
    , c.EMAIL
    , c.Complete_Name