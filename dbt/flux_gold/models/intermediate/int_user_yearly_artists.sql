select period_start, user_name, artist_mbid, sum(plays)::bigint as plays
from {{ ref('int_user_yearly_songs') }}
join {{ ref('stg_song_credits') }} using (song_key)
group by period_start, user_name, artist_mbid
