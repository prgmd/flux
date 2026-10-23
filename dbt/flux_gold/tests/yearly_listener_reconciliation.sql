{{ config(meta={'rule_ids': ['DQ010', 'DQ011']}) }}
with expected as (
    select period_start, sha256(user_name) as listener_key, sum(plays)::bigint as listens,
        count(*) as song_count, max(plays)::double / sum(plays) as top_song_share
    from {{ ref('int_user_yearly_songs') }} group by period_start, user_name
)
select coalesce(e.period_start, a.period_start) as period_start, coalesce(e.listener_key, a.listener_key) as listener_key
from expected e full join {{ ref('yearly_listener_metrics') }} a using (period_start, listener_key)
where e.listens is distinct from a.listens or e.song_count is distinct from a.song_count
    or a.top_song_share is null or not isfinite(a.top_song_share)
    or abs(e.top_song_share - a.top_song_share) > 1e-12
    or not regexp_full_match(a.listener_key, '[0-9a-f]{64}')
