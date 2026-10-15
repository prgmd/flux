with plays as (
    select try_cast(json_extract_string(raw_json, '$.track_metadata.additional_info.ms_played') as bigint) as ms_played
    from {bronze}
    where raw_json like '%"ms_played"%'
)
select count(*) as rows_with_ms_played,
       count(*) filter (where ms_played < 30000) as under_30s,
       count(*) filter (where ms_played < 5000) as under_5s,
       count(*) filter (where ms_played = 0) as zero_ms,
       round(count(*) filter (where ms_played < 30000) * 100.0 / count(*), 2) as under_30s_pct
from plays
