with dim_date as (
    select * from {{ ref('dim_date') }}
)

select 
     id_anio_mes
    , anio
    , mes
    , desc_mes
    , Concat(desc_mes,' ',anio) as Desc_anio_mes
    , Trimestre

from dim_date