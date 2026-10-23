{{ flux_period_config('month') }}
select * from (
with artist_base as (
    {{ listening_metrics(ref('int_user_monthly_artists'), ['artist_mbid']) }}
), names as (
    select artist_mbid, min(artist_name) as artist_name
    from {{ ref('stg_song_credits') }} group by artist_mbid
)
select b.*, a.artist_name,
    rank() over (partition by period_start order by listeners desc) as listener_rank,
    rank() over (partition by period_start order by listens desc) as listen_rank
from artist_base b join names a using (artist_mbid)
) as period_snapshot
{% if var('flux_incremental', false) %}
where period_start in ({{ flux_dates_sql('month') }})
{% endif %}
