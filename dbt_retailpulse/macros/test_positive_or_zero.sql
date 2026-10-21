{% test positive_or_zero(model, column_name) %}
select
    {{ column_name }} as invalid_value,
    count(*) as occurrences
from {{ model }}
where {{ column_name }} < 0
group by 1
{% endtest %}
