select song_key, artist_mbid, artist_name
from {{ source('pinned', 'song_credits') }}
