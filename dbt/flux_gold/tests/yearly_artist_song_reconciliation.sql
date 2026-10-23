{{ config(meta={'rule_ids': ['DQ010', 'DQ011', 'DQ012']}) }}
with expected as (
    select s.period_start, s.song_key, c.artist_mbid, s.listens, s.listeners,
        s.listens::double / a.listens as song_listen_share,
        s.listeners::double / a.listeners as song_listener_reach,
        rank() over (partition by s.period_start, c.artist_mbid order by s.listeners desc) as artist_song_rank
    from {{ ref('yearly_song_metrics') }} s join {{ ref('stg_song_credits') }} c using (song_key)
    join {{ ref('yearly_artist_metrics') }} a on s.period_start = a.period_start and c.artist_mbid = a.artist_mbid
)
select coalesce(e.period_start, a.period_start) as period_start, coalesce(e.song_key, a.song_key) as song_key,
    coalesce(e.artist_mbid, a.artist_mbid) as artist_mbid
from expected e full join {{ ref('yearly_artist_song_metrics') }} a using (period_start, song_key, artist_mbid)
where e.listens is distinct from a.listens or e.listeners is distinct from a.listeners
    or e.artist_song_rank is distinct from a.artist_song_rank
    or a.song_listen_share is null or not isfinite(a.song_listen_share)
    or a.song_listener_reach is null or not isfinite(a.song_listener_reach)
    or abs(e.song_listen_share - a.song_listen_share) > 1e-12
    or abs(e.song_listener_reach - a.song_listener_reach) > 1e-12
