{{ config(meta={'rule_ids': ['DQ010', 'DQ011', 'DQ013']}) }}
with expected as (
    select u.period_start, l.variant, sum(u.plays)::bigint as listens,
        count(distinct u.user_name) as listeners
    from {{ ref('int_user_monthly_songs') }} u join {{ ref('stg_song_labels') }} l using (song_key)
    group by u.period_start, l.variant
)
select coalesce(e.period_start, a.period_start) as period_start, coalesce(e.variant, a.variant) as variant
from expected e full join {{ ref('monthly_variant_metrics') }} a using (period_start, variant)
left join {{ ref('monthly_coverage') }} c on c.period_start = coalesce(e.period_start, a.period_start)
where e.listens is distinct from a.listens or e.listeners is distinct from a.listeners
    or a.listen_share is null or not isfinite(a.listen_share)
    or a.listener_reach is null or not isfinite(a.listener_reach)
    or abs(a.listen_share - e.listens::double / c.period_listens) > 1e-12
    or abs(a.listener_reach - e.listeners::double / c.period_users) > 1e-12
