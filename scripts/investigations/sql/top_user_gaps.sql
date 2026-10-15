with artist as (
    select distinct artist_mbid
    from {dim_artist}
    where name = 'RM' and is_current
),
mine as (
    select f.*
    from {fact} f
    join artist using (artist_mbid)
),
top as (
    select user_id
    from mine
    group by user_id
    order by count(*) desc
    limit 1
),
times as (
    select listened_at, recording_mbid, client_name
    from mine
    where user_id = (select user_id from top)
),
gaps as (
    select epoch(listened_at) - lag(epoch(listened_at)) over (order by listened_at) as gap
    from times
)
select (select count(*) from times) as listens,
       (select count(distinct recording_mbid) from times) as distinct_recordings,
       (select min(listened_at) from times) as first_at,
       (select max(listened_at) from times) as last_at,
       quantile_cont(gap, 0.10) as p10_seconds,
       quantile_cont(gap, 0.50) as median_seconds,
       quantile_cont(gap, 0.90) as p90_seconds
from gaps
