{{ config(
    materialized='incremental',
    unique_key = 'ID_DIM_session',
    tags=['GOLD','INCREMENTAL']
    ) 
    }}

with stg_events as (
    select * from  {{ ref('stg_events')}}
)

select 

        {{ dbt_utils.surrogate_key(['count(e.NK_products)','round(timediff(minute,min(e.Produced_at_time),max(e.Produced_at_Time)),0)','Indicador_compra']) }} as ID_DIM_session,
        e.DD_session,
        count(e.NK_products) as Num_productos_visitados,

        case
            {% for i in range(0,2010,10) %}

                when
                    round(timediff(minute,min(e.Produced_at_time),max(e.Produced_at_Time)),0) < {{ i }}

                 then concat(({{ i }}-10)::varchar,'-',{{i}}::varchar,' minutos')

            {% endfor %}

                else ' +2000 minutos'

        end as Tiempo_sesion_minutos,

        case
            {% for i in range(0,34) %}

                when
                    timediff(hour,min(e.Produced_at_time),max(e.Produced_at_Time)) < {{ i }}

                 then concat(({{ i }}-1)::varchar,'-',{{i}}::varchar,' horas')

            {% endfor %}

                else ' +33 horas'

        end as Tiempo_sesion_horas,

        e.Indicador_compra,
        e.Load_Timestamp

from stg_events e


{% if is_incremental() %}

  where e.Load_timestamp > (select max(Load_Timestamp) from {{ this }})

{% endif %}

group by 

e.Load_Timestamp,
e.DD_session,
e.Indicador_compra