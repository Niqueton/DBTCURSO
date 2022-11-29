{{ 
    config(
        materialized='table', 
        sort='date_day',
        dist='date_day',
        pre_hook="alter session set timezone = 'Europe/Madrid'; alter session set week_start = 7;" 
        ) }}

with date as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2030-01-01' as date)"
    )
    }}  
),

us_holidays as  (
    select * from {{ ref('us_holidays') }}
)



select
      d.date_day
    , year(d.date_day)*10000+month(d.date_day)*100+day(d.date_day) as id_date
    , year(d.date_day) as anio
    , month(d.date_day) as mes
    ,monthname(d.date_day) as desc_mes
    , year(d.date_day)*100+month(d.date_day) as id_anio_mes
    , d.date_day-1 as dia_previo
    , year(d.date_day)||weekiso(d.date_day)||dayofweek(d.date_day) as anio_semana_dia
    , weekiso(d.date_day) as semana
    , case 
            when h.Holiday is null then 'No festivo'
            else h.Holiday
      end as Holiday_indicator
    , case 
            when month(d.date_day)<4 then 'T1'
            when month(d.date_day)<7 then 'T2'
            when month(d.date_day)<10 then 'T3'
            else 'T4'
      end as Trimestre
from date d 
left join
us_holidays h 
on d.date_day=to_date(h.Date)
order by
    date_day desc