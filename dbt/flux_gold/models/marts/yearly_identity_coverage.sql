{{ flux_period_config('year') }}
select * from (
select period_start, identity_status,
    sum(plays)::bigint as listens, count(distinct user_name) as listeners
from {{ ref('int_user_yearly_songs') }}
join {{ ref('stg_song_labels') }} using (song_key)
group by period_start, identity_status
) as period_snapshot
{% if var('flux_incremental', false) %}
where period_start in ({{ flux_dates_sql('year') }})
{% endif %}
