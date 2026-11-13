-- expected_arrival = event_ts + time_to_station (to the second).
-- Returns the rows that break it. Zero rows = PASS.
select *
from {{ ref('stg_arrivals') }}
where abs(datediff(second,
                   dateadd(second, time_to_station, event_ts),
                   expected_arrival)) > 1