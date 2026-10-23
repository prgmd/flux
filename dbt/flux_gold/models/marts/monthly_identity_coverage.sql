select period_start, identity_status,
    sum(plays)::bigint as listens, count(distinct user_name) as listeners
from {{ ref('int_user_monthly_songs') }}
join {{ ref('stg_song_labels') }} using (song_key)
group by period_start, identity_status
