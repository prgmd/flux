select
    date_trunc('month', cast(substr(cast(listened_month as varchar), 1, 7) || '-01' as date))::date as period_start,
    user_name,
    song_key,
    sum(play_count)::bigint as plays
from {{ ref('stg_monthly') }}
join {{ ref('stg_song_dimension') }} using (recording_msid)
group by period_start, user_name, song_key
