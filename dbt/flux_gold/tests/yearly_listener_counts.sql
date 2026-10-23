{{ config(meta={'rule_ids': ['DQ010', 'DQ011']}) }}
with songs as (
    select period_start, song_key, count(distinct user_name) as listeners
    from {{ ref('int_user_yearly_songs') }} group by period_start, song_key
), coverage as (
    select period_start, count(distinct user_name) as period_users,
        count(distinct user_name) filter (where song_key in (
            select song_key from {{ ref('stg_song_credits') }}
        )) as linked_users
    from {{ ref('int_user_yearly_songs') }} group by period_start
)
select 'songs' as model_name, coalesce(e.period_start, a.period_start) as period_start,
    coalesce(e.song_key, a.song_key) as metric_key
from songs e full join {{ ref('yearly_song_metrics') }} a using (period_start, song_key)
where e.listeners is distinct from a.listeners
union all
select 'coverage' as model_name, coalesce(e.period_start, a.period_start) as period_start, '' as metric_key
from coverage e full join {{ ref('yearly_coverage') }} a using (period_start)
where e.period_users is distinct from a.period_users or e.linked_users is distinct from a.linked_users
