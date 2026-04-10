select id_commune,
    commune as name, 
    population,
    latitude as latitude_centre,
    longitude as longitude_centre
from {{ref('stg_communes')}}