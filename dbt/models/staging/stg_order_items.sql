
select
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price
from {{ source('retailx_raw', 'order_items') }}
