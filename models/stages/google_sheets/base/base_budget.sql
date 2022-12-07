with base_budget1 as (
    select * from {{ source('src_google_sheets', 'budget') }}
),
base_budget2 as (
    select 
        ID_BUDGETS,
        MONTH,
        PRODUCT_ID,
        QUANTITY as NUMBER_OF_UNITS_EXPECTED,
        to_date(_fivetran_synced) as Load_Date,
        to_time(_fivetran_synced) as Load_Time 
    from base_budget1
)

select * from base_budget2
