{% test unique_grain(model, columns) %}
select {{ columns | join(', ') }}, count(*) as duplicates
from {{ model }}
group by {{ columns | join(', ') }}
having count(*) > 1
{% endtest %}

{% test nonblank(model, column_name) %}
select * from {{ model }}
where {{ column_name }} is null or trim(cast({{ column_name }} as varchar)) = ''
{% endtest %}

{% test positive_integer(model, column_name) %}
select * from {{ model }}
where {{ column_name }} is null or {{ column_name }} < 1
   or typeof({{ column_name }}) not in ('TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT', 'UHUGEINT')
{% endtest %}
