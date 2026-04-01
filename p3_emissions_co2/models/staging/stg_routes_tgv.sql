/* objectif : rapprocher les gares des routes tgv des gares afin de leur affecteur un code_uic */

/* nettoyage de raw_routes_tgv : on met en minuscule, on remplace saint ar st et montparnasse = vaugirard, puis espaces */

with tgv_depart as (
SELECT 
gare_depart,
TRIM(lower(split_part(
    replace(replace(gare_depart, 'SAINT', 'ST'), 'MONTPARNASSE','VAUGIRARD')
        , '(', 1))) 
    as depart 

    from {{ source('emissions_co2', 'raw_routes_tgv') }} 
),

/* dans la vue stg_communes_gares, on va transformer les valeurs de la colonne nom_gare en minuscule et '-' devient ' '*/
gares as (
    select code_uic, 
    trim(lower(unaccent(replace(regexp_replace(nom_gare, 'GARE-de|Gare de|gare-de|gare de|Gare-du|Gare du|gare-du|gare du', ''), '-' , ' ')))) as nom,
    trim(unaccent(replace(commune_clean, '-', ' '))) as commune,
    latitude_deg, longitude_deg    
    from {{ ref('stg_communes_gares') }}
),

depart as (
select
    t.gare_depart , t.depart, ga.nom as nom_depart, ga.code_uic as code_uic_depart , ga.latitude_deg, ga.longitude_deg                     
from tgv_depart t
inner join gares ga
    on ga.nom = t.depart
),

tgv_arrivee as (
SELECT 
gare_arrivee,
TRIM(lower(split_part(
    replace(replace(gare_depart, 'SAINT', 'ST'), 'MONTPARNASSE','VAUGIRARD')
        , '(', 1))) 
    as arrivee 

    from {{ source('emissions_co2', 'raw_routes_tgv') }} 
),

arrivee as (
select
    a.gare_arrivee , a.arrivee, ga.nom as nom_arrivee, ga.code_uic as code_uic_arrivee , ga.latitude_deg, ga.longitude_deg                     
from tgv_arrivee a
inner join gares ga
    on ga.nom = a.arrivee
)

select distinct on (d.code_uic_depart)
    tgv.gare_depart as depart, tgv.gare_arrivee as arrivee, 
    d.code_uic_depart, a.code_uic_arrivee, 'TGV' as transporteur, 0 as distance, 0 as duree,
    {{ haversine_km('d.latitude_deg', 'd.longitude_deg', 'a.latitude_deg', 'a.longitude_deg') }} as distance_calculee,
    tgv."median(duree_moyenne)" as duree_calculee
from {{ source('emissions_co2', 'raw_routes_tgv') }} as tgv
inner join depart  d on d.gare_depart = tgv.gare_depart
inner join arrivee  a on a.gare_arrivee = tgv.gare_arrivee
order by d.code_uic_depart, distance

