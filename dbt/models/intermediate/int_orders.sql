{{ config(materialized='table') }}

select
    order_id,
    customer_id,
    store_id,
    order_date,
    upper(order_status) as order_status,

    case
        when upper(order_status) = 'COMPLETED' then 1
        else 0
    end as is_completed

from {{ ref('stg_orders') }}