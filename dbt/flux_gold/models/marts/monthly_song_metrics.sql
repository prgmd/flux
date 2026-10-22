with song_base as (
    select
        period_start,
        song_key,
        sum(plays)::bigint as listens,
        count(*) as listeners,
        max(plays) as top_user_listens,
        max(plays)::double / sum(plays) as top_user_share,
        sum(plays)::double / count(*) as plays_per_listener,
        sum(plays::double * plays) / pow(sum(plays)::double, 2) as concentration_hhi,
        pow(sum(plays)::double, 2) / sum(plays::double * plays) as effective_listeners
    from {{ ref('int_user_monthly_songs') }}
    group by period_start, song_key
)
select
    b.*,
    l.* exclude (song_key),
    rank() over (partition by period_start order by listeners desc) as listener_rank,
    rank() over (partition by period_start order by listens desc) as listen_rank
from song_base b
join {{ ref('stg_song_labels') }} l using (song_key)
