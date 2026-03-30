/*on va garder au moins dans un premier temsp uniquement les villes de plus de 10000 habitants => environ 1000 communes
on garde aussi les colonnes nom_standard , population , latitude_centre, longitude_centre */

with source as (
    select nom_standard, 
        LOWER(REPLACE(nom_standard, '-', ' ')) AS commune_clean,
        population, latitude_centre, longitude_centre 
    from {{ source('emissions_co2', 'raw_communes') }}
    where population >= 25000

),
renamed as (
    select
        ROW_NUMBER() OVER (ORDER BY nom_standard) AS id_commune,
        nom_standard as commune, 
        commune_clean,
        population, 
        latitude_centre as latitude,
        longitude_centre as longitude
    from source
)
select * from renamed