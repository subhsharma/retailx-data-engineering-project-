{% snapshot customers_snapshot %}

{{
    config(
        target_schema='retailx.silver',
        unique_key='customer_id',
        strategy='check',
        file_format='iceberg',
        check_cols=[
            'first_name',
            'last_name',
            'email',
            'city'
        ]
    )
}}

select
    customer_id,
    first_name,
    last_name,
    email,
    city,
    signup_date

from {{ source('retailx_raw', 'customers') }}

{% endsnapshot %}
