{{ config(materialized='table') }}

with customers as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        city,
        signup_date

    from {{ ref('dim_customer') }}

),

orders as (

    select
        customer_id,
        count(distinct order_id) as total_orders,
        sum(is_completed) as completed_orders,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date

    from {{ ref('fct_orders') }}

    group by customer_id

),

payments as (

    select
        customer_id,
        sum(amount) as total_payment_amount

    from {{ ref('fct_payments') }}

    where upper(payment_status) = 'SUCCESS'

    group by customer_id

)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.city,
    c.signup_date,

    coalesce(o.total_orders, 0) as total_orders,
    coalesce(o.completed_orders, 0) as completed_orders,
    o.first_order_date,
    o.last_order_date,

    coalesce(p.total_payment_amount, 0) as total_payment_amount

from customers c

left join orders o
    on c.customer_id = o.customer_id

left join payments p
    on c.customer_id = p.customer_id