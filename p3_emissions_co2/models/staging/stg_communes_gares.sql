/* on va associer le n° de commune à la commune de la gare */

WITH gares AS (
    SELECT * ,
        trim(replace (lower(commune), '–', '-')) as name_clean
    FROM {{ source('emissions_co2', 'raw_gares') }} 
),

communes AS (
    SELECT *
    FROM {{ ref('stg_communes') }} 
),

final AS (
    SELECT
        COALESCE(c.id_commune, 0) as id_commune,
        g.code_uic,
        g.commune,
        g.name_clean as commune_clean,
        g.libelle as nom_gare,
        g."c_geo.lat"::float AS latitude_deg,
        g."c_geo.lon"::float AS longitude_deg
    FROM  gares as g
    LEFT JOIN communes c
        ON g.name_clean = lower(c.commune)
)

SELECT * FROM final