
select
    order_id,
    customer_id,
    store_id,
    order_date,
    order_status
from {{ source('retailx_raw', 'orders') }}
