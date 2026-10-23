{{ config(meta={'rule_ids': ['DQ010']}) }}
with expected as (
    select date_trunc('year', cast(m.listened_month || '-01' as date))::date as period_start,
        m.user_name, d.song_key, sum(m.play_count)::bigint as plays
    from {{ ref('stg_monthly') }} m join {{ ref('stg_song_dimension') }} d using (recording_msid)
    group by period_start, m.user_name, d.song_key
)
select coalesce(e.period_start, a.period_start) as period_start,
    coalesce(e.user_name, a.user_name) as user_name, coalesce(e.song_key, a.song_key) as song_key
from expected e full join {{ ref('int_user_yearly_songs') }} a using (period_start, user_name, song_key)
where e.plays is distinct from a.plays
