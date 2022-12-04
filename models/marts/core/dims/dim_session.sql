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
        Indicador_compra

from intermediate_session

{% if is_incremental() %}

  where e.Load_timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}