{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_session',
    tags=['GOLD','INCREMENTAL']
    ) 
    }}

with intermediate_session as (
    select * from  {{ ref('intermediate_session')}}
)



select 

        ID_DIM_session,
        Num_productos_visitados,
        Tiempo_sesion_minutos,
        Tiempo_sesion_horas,
        Indicador_compra,
        {{ fecha_id('to_date(Load_Timestamp)') }} as ID_LOAD_DATE
from intermediate_session

{% if is_incremental() %}

  where {{ fecha_id('to_date(Load_Timestamp)') }} > (select max(ID_LOAD_DATE) from {{ this }})

{% endif %}