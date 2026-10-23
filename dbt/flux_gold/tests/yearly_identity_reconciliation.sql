{{ config(meta={'rule_ids': ['DQ010', 'DQ011']}) }}
with expected as (
    select period_start, identity_status, sum(plays)::bigint as listens,
        count(distinct user_name) as listeners
    from {{ ref('int_user_yearly_songs') }} join {{ ref('stg_song_labels') }} using (song_key)
    group by period_start, identity_status
)
select coalesce(e.period_start, a.period_start) as period_start, coalesce(e.identity_status, a.identity_status) as identity_status
from expected e full join {{ ref('yearly_identity_coverage') }} a using (period_start, identity_status)
where e.listens is distinct from a.listens or e.listeners is distinct from a.listeners
