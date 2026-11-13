
select
    -- envelope
    raw:schema_version::int                 as schema_version,
    raw:event_type::string                  as event_type,
    raw:event_ts::timestamp_ntz             as event_ts,
    raw:ingested_at::timestamp_ntz          as ingested_at,

    -- who / where
    raw:id::string                          as tfl_prediction_id,   -- renamed: "id" is too vague
    raw:line_id::string                     as line_id,
    raw:line_name::string                   as line_name,
    raw:mode_name::string                   as mode_name,
    raw:vehicle_id::string                  as vehicle_id,
    raw:naptan_id::string                   as naptan_id,
    raw:station_name::string                as station_name,
    raw:platform_name::string               as platform_name,
    raw:direction::string                   as direction,
    raw:towards::string                     as towards,
    raw:current_location::string            as current_location,
    raw:destination_name::string            as destination_name,
    raw:destination_naptan_id::string       as destination_naptan_id,

    -- the prediction itself
    raw:time_to_station::int                as time_to_station,
    raw:expected_arrival::timestamp_ntz     as expected_arrival,
    raw:time_to_live::timestamp_ntz         as time_to_live,

    -- load metadata
    snowflake_loaded_at,
    md5(to_varchar(raw))                    as row_fingerprint      -- D20: one ID per whole row

from {{ source('raw', 'arrivals') }}