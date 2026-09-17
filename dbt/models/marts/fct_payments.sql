{{ config(materialized='table') }}

select
    p.payment_id,
    p.order_id,
    o.customer_id,
    o.store_id,
    o.order_date,
    p.payment_method,
    p.payment_status,
    p.amount,
    p.payment_date

from {{ ref('stg_payments') }} p

left join {{ ref('int_orders') }} o
    on p.order_id = o.order_id