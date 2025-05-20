{{
    config(
        materialized='view'
    )
}}

with raw_vessel as (
  select *,
    
  from {{ source('staging', 'ais_staging') }}
  where mmsi is not null
)

select
    -- Identifiers
    {{ dbt_utils.generate_surrogate_key(['mmsi', 'basedatetime']) }} as vessel_trip_id,
    {{ dbt.safe_cast("mmsi", api.Column.translate_type("integer")) }} as mmsi,
    cast(basedatetime as timestamp) as base_datetime,

    -- Navigation info
    cast(lat as float64) as latitude,
    cast(lon as float64) as longitude,
    cast(sog as float64) as speed_over_ground,
    cast(cog as float64) as course_over_ground,
    cast(heading as float64) as heading,

    -- Vessel metadata
    VesselName,
    {{ get_vessel_country('VesselName') }} as vessel_country,
    imo,
    callsign,
    cast(vesseltype as float64) as vessel_type,
    cast(status as float64) as status,
    cast(length as float64) as vessel_length,
    cast(width as float64) as vessel_width,
    cast(draft as float64) as vessel_draft,
    cast(cargo as float64) as cargo_type,
    transceiverclass

from raw_vessel


{% if var('is_test_run', default=true) %}
  limit 100
{% endif %}
