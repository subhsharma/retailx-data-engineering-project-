{{ config(materialized='table') }}

select
    p.product_id,
    p.product_name,
    p.category,
    p.price,
    p.stock_quantity,

    coalesce(s.total_orders, 0) as total_orders,
    coalesce(s.total_quantity_sold, 0) as total_quantity_sold,
    coalesce(s.total_sales, 0) as total_sales,
    coalesce(s.average_selling_price, 0) as average_selling_price

from {{ ref('dim_product') }} p

left join {{ ref('int_product_sales') }} s
    on p.product_id = s.product_id