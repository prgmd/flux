{{ config(meta={'rule_ids': ['DQ010']}) }}
with expected as (
    select date_trunc('year', cast(listened_month || '-01' as date))::date as period_start, sum(play_count)::bigint as n
    from {{ ref('stg_monthly') }} group by period_start
)
{% for model, field in [('yearly_song_metrics', 'listens'), ('yearly_coverage', 'period_listens'), ('yearly_variant_metrics', 'listens'), ('yearly_listener_metrics', 'listens'), ('yearly_identity_coverage', 'listens')] %}
select '{{ model }}' as model_name, coalesce(e.period_start, a.period_start) as period_start
from expected e full join (
    select period_start, sum({{ field }})::bigint as n from {{ ref(model) }} group by period_start
) a using (period_start)
where e.n is distinct from a.n
{% if not loop.last %}union all{% endif %}
{% endfor %}
