{% macro cents_to_dollars(column_name, decimal_places=2) -%}
    round(cast(({{ column_name }} / 100.0) as numeric(16, {{ decimal_places }})), {{ decimal_places }})
{%- endmacro %}
