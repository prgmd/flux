with pairs as (
    select artist_mbid, user_id, count(*) as n
    from {fact}
    where artist_mbid is not null
    group by artist_mbid, user_id
),
names as (
    select artist_mbid, any_value(name) as artist_name
    from {dim_artist}
    where is_current
    group by artist_mbid
),
by_artist as (
    select artist_mbid,
           sum(n) as listens,
           count(*) as users,
           max(n) as top_user_listens
    from pairs
    group by artist_mbid
)
select coalesce(names.artist_name, by_artist.artist_mbid) as artist,
       by_artist.listens,
       by_artist.users,
       round(by_artist.top_user_listens * 100.0 / by_artist.listens, 1) as top_user_pct
from by_artist
left join names using (artist_mbid)
order by by_artist.listens desc
limit 20
