with base_events as (
    select * from {{ ref('base_events') }}
),

base_users as (
    select * from {{ ref('base_users_snapshot') }}
),

stg_addresses as (
    select * from {{ ref('stg_addresses') }}
),

intermedio as (

    select 
        session_id as ses,
        case
            when event_type='add_to_cart' then 1
            else 0
        end as Add_to_cart,
                case
            when event_type='checkout' then 1
            else 0
        end as Checkout,
                case
            when event_type='package_shipped' then 1
            else 0
        end as Package_shipped
    from base_events
),


eventati as (
    select
        e.SESSION_ID,
        min(e.Produced_at_Time) as Tiempo_comienzo,
        max(e.Produced_at_Time) as Tiempo_final,
        max(e.Produced_at_Date) as Fecha,
        timediff(minute,min(e.Produced_at_Time),max(e.Produced_at_Time)) as Tiempo_sesion_minutos,
        sum(i.Add_to_cart) as  Add_to_cart,
        sum(i.Checkout) as Checkout,
        sum(i.Package_shipped) as Package_shipped,
        count(distinct e.PAGE_URL) as Num_web_visitadas,
        e.USER_ID

    from
    base_events e
    inner JOIN 
    intermedio i
    on e.SESSION_ID=i.SES
    group by session_id,user_id
)


select 
        e.SESSION_ID,
        e.Tiempo_comienzo,
        e.Tiempo_final,
        e.Tiempo_sesion_minutos,
        e.Fecha,
        e.Add_to_cart,
        e.Checkout,
        e.Package_shipped,
        e.Num_web_visitadas,
        u.ID_DIM_users,
		u.LAST_NAME ,
		u.PHONE_NUMBER ,
		u.FIRST_NAME ,
		u.EMAIL ,
        Concat(a.STATE,',',a.city,',',a.ADDRESS,',',a.ZIPCODE) as Direccion_principal
from 
eventati e 
left join 
base_users u 
on e.USER_ID=u.NK_users
inner join 
stg_addresses a
on u.ADDRESS_ID=a.NK_address