/*on va garder les liaisons qui ne comprennent pas l'international (on garde TGV, Intercités, TER*/

with source as (
    select *
    from {{ source('emissions_co2', 'raw_trains') }}
    where "Transporteur" <> 'International'

),

union_trajets as (

    select 
        "Transporteur" as transporteur,
        "Origine" as depart, 
        "Destination" as arrivee,
        "Origine_uic"::text as code_uic_depart,
        "Destination_uic"::text as code_uic_arrivee,
        "Distance entre les gares"::float as distance,
        0 as duree,
        0 as distance_calculee,
        0 as duree_calculee
    from source
    union all
    select 
        t.transporteur, 
        t.depart, 
        t.arrivee, 
        t.code_uic_depart::text, 
        t.code_uic_arrivee::text, 
        t.distance,
        t.duree,
        t.distance_calculee::float,
        t.duree_calculee
    from {{ref('stg_routes_tgv')}} t
    union all
    select 
        i.transporteur, 
        i.depart, 
        i.arrivee, 
        i.code_uic_depart::text,
        i.code_uic_arrivee::text,
        i.distance,
        i.duree,
        i.distance_calculee::float,
        i.duree_calculee
    from {{ref('stg_routes_intercites')}} i
)

select distinct on (code_uic_depart, code_uic_arrivee)
*
from union_trajets 
order by code_uic_depart, code_uic_arrivee