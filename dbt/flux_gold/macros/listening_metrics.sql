{% macro listening_metrics(source, keys) %}
select
    period_start,
    {{ keys | join(', ') }},
    sum(plays)::bigint as listens,
    count(*) as listeners,
    max(plays) as top_user_listens,
    max(plays)::double / sum(plays) as top_user_share,
    sum(plays)::double / count(*) as plays_per_listener,
    sum(plays::double * plays) / pow(sum(plays)::double, 2) as concentration_hhi,
    pow(sum(plays)::double, 2) / sum(plays::double * plays) as effective_listeners
from {{ source }}
group by period_start, {{ keys | join(', ') }}
{% endmacro %}
