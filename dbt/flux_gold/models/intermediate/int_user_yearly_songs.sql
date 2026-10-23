select date_trunc('year', period_start)::date as period_start,
    user_name, song_key, sum(plays)::bigint as plays
from {{ ref('int_user_monthly_songs') }}
group by date_trunc('year', period_start)::date, user_name, song_key
