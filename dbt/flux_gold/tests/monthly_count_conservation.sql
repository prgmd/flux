{{ config(meta={'rule_ids': ['DQ010']}) }}
with expected as (
    select cast(listened_month || '-01' as date) as period_start, sum(play_count)::bigint as n
    from {{ ref('stg_monthly') }} group by period_start
), songs as (
    select period_start, sum(listens)::bigint as n from {{ ref('monthly_song_metrics') }} group by period_start
), coverage as (
    select period_start, period_listens as n from {{ ref('monthly_coverage') }}
)
select coalesce(e.period_start, s.period_start, c.period_start) as period_start
from expected e full join songs s using (period_start) full join coverage c using (period_start)
where e.n is distinct from s.n or e.n is distinct from c.n
