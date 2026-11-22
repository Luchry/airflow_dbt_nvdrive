{% test length_equals(model, column_name, length) %}
select {{ column_name }}
from {{ model }}
where length({{ column_name }}) <> {{ length }}
{% endtest %}
