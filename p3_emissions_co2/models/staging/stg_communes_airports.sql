/* on va associer le n° de commune à la commune de l'aéroport */

WITH airports AS (
    SELECT * 
    FROM {{ ref('stg_airports') }} a
),

communes AS (
    SELECT *
    FROM {{ ref('stg_communes') }} c
),

final AS (
    SELECT
        COALESCE(c.id_commune, 0) AS id_commune,
        a.icao_code,
        a.iata_code,
        a.commune,
        a.commune_clean,
        c.commune as comune_commune,
        nom_airport,
        a.latitude_deg,
        a.longitude_deg,
        a.type
    FROM airports a
    LEFT JOIN communes c
        ON lower(a.commune_clean) = lower(c.commune)
)

SELECT * FROM final