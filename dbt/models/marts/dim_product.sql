{{ config(materialized='table') }}

with products as (

    select
        product_id,
        product_name,
        category,
        price,
        stock_quantity,

        row_number() over (
            partition by product_id
            order by product_id
        ) as rn

    from {{ ref('stg_products') }}

)

select
    product_id,
    product_name,
    category,
    price,
    stock_quantity

from products

where rn = 1