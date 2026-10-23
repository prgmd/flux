select period_start, user_name, variant, sum(plays)::bigint as plays
from {{ ref('int_user_yearly_songs') }}
join {{ ref('stg_song_labels') }} using (song_key)
group by period_start, user_name, variant
