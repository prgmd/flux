{{ config(meta={'rule_ids': ['DQ010', 'DQ011', 'DQ012']}) }}
with users as (
    select period_start, user_name, artist_mbid, sum(plays)::bigint as plays
    from {{ ref('int_user_yearly_songs') }} join {{ ref('stg_song_credits') }} using (song_key)
    group by period_start, user_name, artist_mbid
), expected as (
    select period_start, artist_mbid, sum(plays)::bigint as listens,
        count(*) as listeners, max(plays) as top_user_listens
    from users group by period_start, artist_mbid
), ranked as (
    select *, rank() over (partition by period_start order by listeners desc) as listener_rank,
        rank() over (partition by period_start order by listens desc) as listen_rank
    from expected
)
select coalesce(e.period_start, a.period_start) as period_start, coalesce(e.artist_mbid, a.artist_mbid) as artist_mbid
from ranked e full join {{ ref('yearly_artist_metrics') }} a using (period_start, artist_mbid)
where e.listens is distinct from a.listens or e.listeners is distinct from a.listeners
    or e.top_user_listens is distinct from a.top_user_listens
    or e.listener_rank is distinct from a.listener_rank or e.listen_rank is distinct from a.listen_rank
