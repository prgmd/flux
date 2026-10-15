with per_day as (
    select user_name, cast(listened_at as date) as day, count(*) as n
    from {silver}
    group by user_name, day
)
select count(*) filter (where n > 500) as user_days_over_500,
       count(*) filter (where n > 1000) as user_days_over_1000,
       count(*) filter (where n > 2000) as user_days_over_2000,
       count(*) filter (where n > 2880) as user_days_over_physical_limit,
       count(distinct user_name) filter (where n > 1000) as users_with_a_day_over_1000,
       sum(n) filter (where n > 1000) as listens_on_those_days,
       round(sum(n) filter (where n > 1000) * 100.0 / sum(n), 2) as pct_of_all_listens,
       max(n) as max_day
from per_day
