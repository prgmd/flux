{{ flux_period_config('year') }}
select * from (
with variant_base as (
    {{ listening_metrics(ref('int_user_yearly_variants'), ['variant']) }}
)
select b.*,
    listens::double / period_listens as listen_share,
    listeners::double / period_users as listener_reach
from variant_base b join {{ ref('yearly_coverage') }} using (period_start)
) as period_snapshot
{% if var('flux_incremental', false) %}
where period_start in ({{ flux_dates_sql('year') }})
{% endif %}
