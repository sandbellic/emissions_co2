/*on va garder les liaisons qui ne comprennent pas l'international (on garde TGV, Intercités, TER*/

with source as (
    select *
    from {{ source('emissions_co2', 'raw_trains') }}
    where "Transporteur" <> 'International'

),
renamed as (
    select
        "Transporteur" as transporteur,
        "Origine" as source, 
        "Destination" as destination,
        "Distance entre les gares" as distance_train,
        "Train - Empreinte carbone (kgCO2e)" as co2e_train,
        "Distance routière" as distance_route,
        "Autocar longue distance - Empreinte carbone (kgCO2e)" as co2e_autocar,
        "Distance aérienne" as distance_air,
        "Avion - Empreinte carbone (kgCO2e)" as co2e_avion,

        /*création directe des nouvelles colonnes, ne va pas modifier une table
        il recrée une vue / table à partir d'un select => dans dbt QUE DES SELECT */
        CASE 
            WHEN "Origine" ILIKE '%Paris%' THEN 'Paris'
            ELSE "Origine"
        END AS ville_origine,

        CASE 
            WHEN "Destination" ILIKE '%Paris%' THEN 'Paris'
            ELSE "Destination"
        END AS ville_destination

    FROM source
)


SELECT * FROM renamed