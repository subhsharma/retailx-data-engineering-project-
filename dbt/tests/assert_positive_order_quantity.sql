select
    order_item_id,
    quantity

from {{ ref('stg_order_items') }}

where quantity <= 0