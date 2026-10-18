with pairs as (
    select artist_mbid, user_id, count(*) as n
    from {fact}
    where artist_mbid is not null
    group by artist_mbid, user_id
), by_artist as (
    select artist_mbid, sum(n)::bigint as listens,
           count(*) as listeners, max(n) as top_user_listens
    from pairs
    group by artist_mbid
), ranked as (
    select *, rank() over (order by listens desc) as play_rank,
           rank() over (order by listeners desc) as listener_rank
    from by_artist
), names as (
    select artist_mbid, min(name) as artist_name
    from {dim_artist}
    where is_current
    group by artist_mbid
)
select artist_mbid, coalesce(artist_name, artist_mbid) as artist,
       listens, listeners, play_rank, listener_rank, top_user_listens,
       round(top_user_listens * 100.0 / listens, 2) as top_user_pct,
       round(listens * 1.0 / listeners, 2) as plays_per_listener
from ranked left join names using (artist_mbid)
order by play_rank, artist_mbid
