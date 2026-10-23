select period_start, sha256(user_name) as listener_key,
    sum(plays)::bigint as listens, count(*) as song_count,
    max(plays)::double / sum(plays) as top_song_share
from {{ ref('int_user_yearly_songs') }}
group by period_start, user_name
