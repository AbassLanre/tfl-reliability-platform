
## Envelope (all topics)

| field          | type          | required | source                       |
|----------------|---------------|----------|------------------------------|
| schema_version | int           | yes      | producer (constant, starts 1)|
| event_type     | string        | yes      | producer (e.g. "arrival")    |
| ingested_at    | string (ISO8601 UTC) | yes | producer clock            |
| event_ts     | string | yes      | timestamp |
<!-- | lastUpdate     | string | yes      | producer | -->

## tfl.arrivals payload

| field       | type   | required | source (TfL field) |
|-------------|--------|----------|--------------------|
| line_id     | string | yes      | lineId (also Kafka key) |
| time_to_station | int | yes   | timeToStation      |
| id          | string | yes      | id                       |
| vehicle_id     | string | yes      | vehicleId  |
| naptan_id     | string | yes      | naptanId |
| station_name     | string | yes      | stationName |
| line_name     | string | yes      | lineName |
| platform_name     | string | yes      |platformName |
| current_location     | string | yes      | currentLocation |
| towards     | string | no      | towards |
| expected_arrival     | string | yes      | expectedArrival |
| time_to_live     | string | yes      | timeToLive |
| mode_name     | string | yes      | modeName |
| direction     | string | no      | direction |
| destination_name     | string | no      | destinationName |
| destination_naptan_id     | string | no      | destinationNaptanId |

## tfl.line-status payload

| field       | type   | required | source (TfL field) |
|-------------|--------|----------|--------------------|
| line_id     | string | yes      | id (kafka key)|
| status_severity | int | yes   | statusSeverity      |
| status_severity_description | string | no      | statusSeverityDescription                  |
| valid_from     | string | no      | validityPeriods |
| valid_to     | string | no      | validityPeriods |
| reason     | string | no      | reason |

## tfl.disruptions payload

| field       | type   | required | source (TfL field) |
|-------------|--------|----------|--------------------|
| line_id     | string | yes      | producer (from polled line) |
| category     | string | yes      | category |
| category_description | string | no   | categoryDescription      |
| description          | string | no      | description                       |
| affected_routes     | string | no      | affectedRoutes |
| affected_stops     | string | no      | affectedStops |
| closure_text     | string | no      | closureText |
