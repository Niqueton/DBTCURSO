with base_promos1 as (
    select * from {{ source('src_sql_server', 'promos') }}
),
base_promos2 as (
    select 
        ID_DIM_promos,
        promo_id as Promotion_Name,
        discount as Order_discount_in_Dollars,
        status,
        to_date(_fivetran_synced) as Load_Date
        to_time(_fivetran_synced) as Load_Time
    from promos1
    where _fivetran_deleted=False

)

select * from base_promos2