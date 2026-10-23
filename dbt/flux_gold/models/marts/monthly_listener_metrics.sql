{{ flux_period_config('month') }}
select * from (
select period_start, sha256(user_name) as listener_key,
    sum(plays)::bigint as listens, count(*) as song_count,
    max(plays)::double / sum(plays) as top_song_share
from {{ ref('int_user_monthly_songs') }}
group by period_start, user_name
) as period_snapshot
{% if var('flux_incremental', false) %}
where period_start in ({{ flux_dates_sql('month') }})
{% endif %}
