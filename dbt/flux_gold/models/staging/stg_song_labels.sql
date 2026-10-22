select song_key, song_name, observed_artist, variant, identity_status, source_msids
from {{ source('pinned', 'song_labels') }}
