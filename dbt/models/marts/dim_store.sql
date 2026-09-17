{{ config(materialized='table') }}

select
    store_id,
    store_name,
    city,
    store_type

from {{ ref('stg_stores') }}