
select
    line_id,
    line_name,
    mode
from {{ ref('tube_lines') }}
