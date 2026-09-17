{% macro calculate_sales(quantity_column, price_column) %}

    ({{ quantity_column }} * {{ price_column }})

{% endmacro %}