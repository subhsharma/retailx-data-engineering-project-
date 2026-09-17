
select
    store_id,
    store_name,
    city,
    store_type
from {{ source('retailx_raw', 'stores') }}
