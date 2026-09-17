
select
    product_id,
    product_name,
    category,
    price,
    stock_quantity
from {{ source('retailx_raw', 'products') }}
