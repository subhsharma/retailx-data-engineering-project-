{{ config(materialized='table') }}

select
    o.customer_id,
    count(distinct o.order_id) as total_orders,
    sum(case when o.is_completed = 1 then 1 else 0 end) as completed_orders,
    min(o.order_date) as first_order_date,
    max(o.order_date) as last_order_date

from {{ ref('int_orders') }} o

group by
    o.customer_id