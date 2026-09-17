{{ config(materialized='table') }}

select
    oi.product_id,
    count(distinct oi.order_id) as total_orders,
    sum(oi.quantity) as total_quantity_sold,
    sum(oi.quantity * oi.unit_price) as total_sales,
    avg(oi.unit_price) as average_selling_price

from {{ ref('stg_order_items') }} oi

group by
    oi.product_id