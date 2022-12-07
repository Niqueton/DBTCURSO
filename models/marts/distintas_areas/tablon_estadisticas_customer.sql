with facts_web_interaction as (
    select * from {{ ref('facts_web_interaction') }}
),

facts_item_sales as (
    select * from {{ ref('facts_item_sales') }}
),

dim_customer_historica as (
    select * from {{ ref('dim_customer_historica') }}
),

dim_shipping_address as (
    select * from {{ ref('dim_shipping_addresses') }}
)

select 

      'User_Data'
    , 'Dirección principal'
    , 'Numero_Pedidos'
    , 'Numero_de_items'
    ,' Numero_de_items_distintos'
    , 'Item_favorito (si existe)'
    , 'Gasto_total'
    , 'Gasto_medio_por_pedido'
    , 'Numero_sesiones_web'
    , 'Ratio_de_finalización_en_compra_web'
    , 'Loyalty_indicator'
    , 'Economic_level'
    ,'Tiempo_total_web'
    ,' Descuento_total'



