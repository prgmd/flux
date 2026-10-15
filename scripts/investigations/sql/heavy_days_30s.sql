with nxt as (
    select user_name,
           listened_at,
           epoch(lead(listened_at) over (partition by user_name order by listened_at)) - epoch(listened_at) as gap_to_next
    from {silver}
),
per_day as (
    select user_name,
           cast(listened_at as date) as day,
           count(*) as all_listens,
           count(*) filter (where gap_to_next >= 30) as listens_30s_or_more
    from nxt
    group by user_name, day
)
select count(*) filter (where all_listens > 1000) as days_all_over_1000,
       count(*) filter (where listens_30s_or_more > 1000) as days_30s_over_1000,
       count(*) filter (where listens_30s_or_more > 2000) as days_30s_over_2000,
       count(distinct user_name) filter (where listens_30s_or_more > 1000) as users_30s_over_1000,
       sum(listens_30s_or_more) filter (where listens_30s_or_more > 1000) as listens_on_those_days,
       max(listens_30s_or_more) as max_30s_day
from per_day
