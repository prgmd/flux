{{ flux_period_config('year') }}
select * from (
select
    period_start,
    sum(plays)::bigint as period_listens,
    count(distinct user_name) as period_users,
    coalesce(sum(plays) filter (where linked), 0)::bigint as linked_listens,
    count(distinct user_name) filter (where linked) as linked_users,
    coalesce(sum(plays) filter (where linked), 0)::double / sum(plays) as artist_coverage
from (
    select *, song_key in (select song_key from {{ ref('stg_song_credits') }}) as linked
    from {{ ref('int_user_yearly_songs') }}
)
group by period_start
) as period_snapshot
{% if var('flux_incremental', false) %}
where period_start in ({{ flux_dates_sql('year') }})
{% endif %}
