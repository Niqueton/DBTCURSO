with lista as(
    select 0 as Numero

    {% for i in range(1,60) %}

    union

    select {{i}}

    {% endfor %}
),
hora as (
    select * from lista where Numero<24
),
minuto as (
    select * from lista
),
segundo as (
    select * from lista
)

select 

      to_time(concat(h.Numero,':',m.Numero,':',s.Numero)) as time
    , h.Numero as Hora
    , m.Numero as Minuto
    , s.Numero as Segundo
    , h.Numero*100000+m.Numero*1000+s.Numero as ID_TIME
    , case
        when h.Numero<7 then 'Madrugada'
        when h.Numero<12 then 'Mañana'
        when h.Numero<16 then 'Mediodía'
        when h.Numero<21 then 'Tarde'
        else 'Noche'
      end as Parte_del_dia

from hora h
cross join minuto m 
cross join segundo s