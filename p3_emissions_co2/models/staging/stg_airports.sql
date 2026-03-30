/*on va garder au moins dans un premier uniquement les aéroports large et medium,
ainsi que ceux ayant un code OACI et IATA non null */

with source as (
    select *,
        trim(replace(replace (lower(name), '–', '-'), 'airport', '')) as name_clean
    from {{ source('emissions_co2', 'raw_airports') }}
    where type in ('large_airport', 'medium_airport') and icao_code is not null and iata_code is not null

),
cleaned as (
    select
        icao_code,
        iata_code,
        municipality as commune,
        ------les noms des aéroports ne sont pas normalisés, on va distinguer plusieurs cas
        case 
            when lower(municipality) ~ '^paris' then 'paris'
            ----cas de Le Touquet, La Plagne, ...
            when lower(name) ~ '^(le|la|les)' then
                intermediaire = SPLIT_PART(SPLIT_PART(name_clean, ' ', 2), '-',1)
                SPLIT_PART(name_clean, ' ', 1) || ' ' || SPLIT_PART(SPLIT_PART(name_clean, ' ', 2), '-',1)
            when lower(name_clean) ~ '^(saint |sainte )' then           
                SPLIT_PART(name_clean, ' ', 1) || ' ' || SPLIT_PART(SPLIT_PART(name_clean, ' ', 2), '-',1)
            when lower(name_clean) ~ '^(saint-|sainte-)' then           
                SPLIT_PART(name_clean, '-', 1) || '-' || SPLIT_PART(SPLIT_PART(name_clean, '-', 2), ' ',1)                        
            ------dans tous les autres cas , on prend tout avant espace OU tiret
            else SPLIT_PART(SPLIT_PART(name_clean, ' ', 1), '-',1)
        end as commune_clean,
        -------
        name as nom_airport,
        latitude_deg,
        longitude_deg, 
        type
    from source
)

select * from cleaned