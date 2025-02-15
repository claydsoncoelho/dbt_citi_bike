select * 
from {{ source('source_citi_bike', 'trips') }}
where tripduration <=0