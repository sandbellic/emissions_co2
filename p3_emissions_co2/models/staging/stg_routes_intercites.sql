/* objectif : rapprocher les gares des routes intercites des gares afin de leur affecteur un code_uic */

/* nettoyage de raw_routes_intercites : on met en minuscule, on enlève les accents,  puis espaces*/
/* on a des gares toulouse/rodez => dans un premier temps on prend toulouse*/

with intercites_depart as (
    SELECT 
    TRIM(lower(replace(split_part(unaccent(depart), '/',1), '-', ' ')))
        as g_depart ,
        depart as gare_depart
        from {{ source('emissions_co2', 'raw_routes_intercites') }} 
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
    t.gare_depart , t.g_depart, ga.nom as nom_depart, ga.code_uic as code_uic_depart , ga.latitude_deg, ga.longitude_deg                  
from intercites_depart t
inner join gares ga
    on ga.nom = t.g_depart
union 
select
    t.gare_depart , t.g_depart, ga.nom as nom_depart, ga.code_uic as code_uic_depart , ga.latitude_deg, ga.longitude_deg                     
from intercites_depart t
inner join gares ga
    on ga.commune = t.g_depart
),

intercites_arrivee as (
SELECT 
    TRIM(lower(replace(split_part(unaccent(arrivee), '/',1), '-', ' ')))
        as g_arrivee,
        arrivee as gare_arrivee
        from {{ source('emissions_co2', 'raw_routes_intercites') }} 
 ), 


arrivee as (
select
    a.gare_arrivee , a.g_arrivee, ga.nom as nom_arrivee, ga.code_uic as code_uic_arrivee, ga.latitude_deg, ga.longitude_deg                      
from intercites_arrivee a
inner join gares ga
    on ga.nom = a.g_arrivee
union
select
    a.gare_arrivee , a.g_arrivee, ga.nom as nom_arrivee, ga.code_uic as code_uic_arrivee, ga.latitude_deg, ga.longitude_deg                       
from intercites_arrivee a
inner join gares ga
    on ga.commune = a.g_arrivee
)

select distinct on (d.code_uic_depart)
    int.depart, int.arrivee, 
    d.code_uic_depart, a.code_uic_arrivee, 'Intercites' as transporteur, 0 as distance, 0 as duree,
    {{ haversine_km('d.latitude_deg', 'd.longitude_deg', 'a.latitude_deg', 'a.longitude_deg') }} as distance_calculee,
    0 as duree_calculee
from {{ source('emissions_co2', 'raw_routes_intercites') }} as int
inner join depart d on d.gare_depart = int.depart
inner join arrivee a on a.gare_arrivee = int.arrivee
order by d.code_uic_depart, distance

