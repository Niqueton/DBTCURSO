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
),

previo as (
    select 
    it.ID_DIM_CUSTOMER,
    count(distinct it.DD_orders) as Numero_de_pedidos,
    sum(Quantity_sold) as Numero_total_items,
    count(distinct ID_DIM_products) as Numero_items_distintos,
    sum(ORDER_TOTAL_USD) as  Gasto_total_de_cliente_USD,
    sum(Proportional_discount_USD) as Descuento_total_USD

    from facts_item_sales it 
    left join facts_order_tracking o
    on it.DD_orders=o.DD_orders
    group by it.ID_DIM_CUSTOMER
),

ev as (
    select 
    count(distinct DD_session) as Numero_sesiones_web,
    ID_DIM_CUSTOMER

    from facts_web_interaction
    group by ID_DIM_CUSTOMER
)


select 
      c.ID_DIM_customer
    , Main_Address
    , EMAIL
    , Complete_Name
    , Numero_de_pedidos
    , Numero_total_items
    , Numero_items_distintos
    , round(Gasto_total_de_cliente_USD,2) as Gasto_total_de_cliente_USD
    , case 
            when Numero_de_pedidos=0 then 0
            else round((Gasto_total_de_cliente_USD/Numero_de_pedidos),2)
      end as Gasto_medio_pedido_USD

    , Numero_sesiones_web
    , case 
            when Numero_sesiones_web=0 then 'No aplica'
            else concat(round((100*Numero_de_pedidos/Numero_sesiones_web),0)::varchar,'%')
      end as Ratio_de_finalizacon_en_compra
    , round(Descuento_total_USD,2) as Descuento_total_USD
from dim_customer_historica c

left join previo p 
on c.ID_DIM_CUSTOMER=p.ID_DIM_CUSTOMER

left join ev 
on c.ID_DIM_CUSTOMER=ev.ID_DIM_CUSTOMER

