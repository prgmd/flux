select s.*, c.artist_mbid, c.artist_name,
    s.listens::double / a.listens as song_listen_share,
    s.listeners::double / a.listeners as song_listener_reach,
    rank() over (partition by s.period_start, c.artist_mbid order by s.listeners desc) as artist_song_rank
from {{ ref('yearly_song_metrics') }} s
join {{ ref('stg_song_credits') }} c using (song_key)
join {{ ref('yearly_artist_metrics') }} a
    on s.period_start = a.period_start and c.artist_mbid = a.artist_mbid
