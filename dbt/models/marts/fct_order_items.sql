{{ config(materialized='table') }}

select
    oi.order_item_id,
    oi.order_id,
    oi.product_id,
    o.customer_id,
    o.store_id,
    o.order_date,
    oi.quantity,
    oi.unit_price,

    {{ calculate_sales('oi.quantity', 'oi.unit_price') }} as line_total

from {{ ref('stg_order_items') }} oi

left join {{ ref('int_orders') }} o
    on oi.order_id = o.order_id