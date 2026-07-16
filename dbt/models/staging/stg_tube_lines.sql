
select
    line_id,
    line_name,
    mode
from {{ source('raw', 'tube_lines') }}
