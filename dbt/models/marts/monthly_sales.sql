{{ config(materialized='table') }}

select
    date_trunc('month', o.order_date) as sales_month,

    count(distinct o.order_id) as total_orders,

    count(
        distinct case
            when upper(o.order_status) = 'COMPLETED'
            then o.order_id
        end
    ) as completed_orders,

    sum(oi.quantity) as total_quantity_sold,

    sum(oi.line_total) as total_sales,

    avg(oi.line_total) as average_order_item_value

from {{ ref('fct_orders') }} o

left join {{ ref('fct_order_items') }} oi
    on o.order_id = oi.order_id

group by
    date_trunc('month', o.order_date)

order by
    sales_month