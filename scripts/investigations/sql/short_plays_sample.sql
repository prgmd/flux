select _source_dump as dump,
       user_name,
       json_extract_string(raw_json, '$.track_metadata.artist_name') as artist,
       json_extract_string(raw_json, '$.track_metadata.track_name') as track,
       try_cast(json_extract_string(raw_json, '$.track_metadata.additional_info.ms_played') as bigint) as ms_played,
       try_cast(json_extract_string(raw_json, '$.track_metadata.additional_info.duration_ms') as bigint) as duration_ms
from {bronze}
where raw_json like '%"ms_played"%'
  and try_cast(json_extract_string(raw_json, '$.track_metadata.additional_info.ms_played') as bigint) < 30000
limit 20
