-- One row per distinct TfL prediction, junk vehicle '000' removed.
-- Table, not view: the dedupe is heavy, so do it once.

{{ config(materialized='table') }}

with numbered as (
    select *, row_number() over (partition by line_id, vehicle_id, naptan_id, event_ts
                                 order by ingested_at) as rn
    from {{ ref('stg_arrivals') }}
    where vehicle_id != '000'
)

select 
    md5(concat_ws('|', line_id, vehicle_id, naptan_id, to_varchar(event_ts, 'YYYY-MM-DD HH24:MI:SS.FF9'))) as prediction_key,
    * exclude (rn)     -- drop the helper column
from numbered
where rn = 1              -- now rn exists, so WHERE can see it