{{ config(materialized='table') }}

with orders as (

    select
        store_id,
        count(distinct order_id) as total_orders,
        sum(is_completed) as completed_orders

    from {{ ref('fct_orders') }}

    group by store_id

),

sales as (

    select
        o.store_id,
        sum(oi.line_total) as total_sales,
        sum(oi.quantity) as total_quantity_sold

    from {{ ref('fct_order_items') }} oi

    left join {{ ref('fct_orders') }} o
        on oi.order_id = o.order_id

    group by o.store_id

)

select
    s.store_id,
    s.store_name,
    s.city,
    s.store_type,

    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.completed_orders, 0) as completed_orders,
    coalesce(sa.total_quantity_sold, 0) as total_quantity_sold,
    coalesce(sa.total_sales, 0) as total_sales

from {{ ref('dim_store') }} s

left join orders o
    on s.store_id = o.store_id

left join sales sa
    on s.store_id = sa.store_id