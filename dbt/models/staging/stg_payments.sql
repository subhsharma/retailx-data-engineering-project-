
select
    payment_id,
    order_id,
    payment_method,
    payment_status,
    amount,
    payment_date
from {{ source('retailx_raw', 'payments') }}
