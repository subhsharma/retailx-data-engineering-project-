{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge',
    file_format='iceberg'
) }}

select
    o.order_id,
    o.customer_id,
    o.store_id,
    o.order_date,
    o.order_status,
    o.is_completed,

    coalesce(c.total_orders, 0) as customer_total_orders

from {{ ref('int_orders') }} o

left join {{ ref('int_customer_orders') }} c
    on o.customer_id = c.customer_id

{% if is_incremental() %}

where o.order_date >= (
    select coalesce(max(order_date), to_date('1900-01-01'))
    from {{ this }}
)

{% endif %}
