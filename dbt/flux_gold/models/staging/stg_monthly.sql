select user_name, recording_msid, listened_month, play_count
from {{ source('pinned', 'monthly') }}
