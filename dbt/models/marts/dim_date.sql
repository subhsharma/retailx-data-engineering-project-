{{ config(materialized='table') }}

with date_spine as (

    select
        date_add(to_date('2024-01-01'), cast(id as int)) as date_day

    from range(1096)

)

select
    date_day,
    year(date_day) as year,
    quarter(date_day) as quarter,
    month(date_day) as month,
    date_format(date_day, 'MMMM') as month_name,
    weekofyear(date_day) as week,
    day(date_day) as day,
    dayofweek(date_day) as day_of_week

from date_spine

where date_day <= to_date('2026-12-31')
