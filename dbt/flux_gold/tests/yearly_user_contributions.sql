{{ config(meta={'rule_ids': ['DQ010']}) }}
with expected as (
    select date_trunc('year', cast(m.listened_month || '-01' as date))::date as period_start,
        m.user_name, d.song_key, sum(m.play_count)::bigint as plays
    from {{ ref('stg_monthly') }} m join {{ ref('stg_song_dimension') }} d using (recording_msid)
    group by period_start, m.user_name, d.song_key
), actual as (
    select period_start, user_name, song_key, plays from {{ ref('int_user_yearly_songs') }}
)
(select * from expected except all select * from actual)
union all
(select * from actual except all select * from expected)
