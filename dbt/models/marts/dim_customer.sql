{{ config(materialized='table') }}

with customers as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        city,
        signup_date,

        row_number() over (
            partition by customer_id
            order by signup_date desc
        ) as rn

    from {{ ref('stg_customers') }}

)

select
    customer_id,
    first_name,
    last_name,
    email,
    city,
    signup_date

from customers

where rn = 1