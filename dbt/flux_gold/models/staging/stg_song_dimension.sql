select recording_msid, recording_mbid, status, song_key, song_name, observed_artist, variant
from {{ source('pinned', 'song_dimension') }}
