"""Silver의 각 칸이 원본 JSON의 어느 경로에서 왔는지. DuckDB와 Spark를 부르지 않는다.

탐색 화면이 "이 값은 어디서 왔나"를 보여 줄 때 쓴다. 경로는 flux.silver.build가 실제로 읽는 것과
같아야 한다. 어긋나면 화면이 거짓을 보여 주므로 테스트가 build.py의 글자와 대조한다.
"""

RECORDING_PATHS = (
    "$.track_metadata.mbid_mapping.recording_mbid",
    "$.track_metadata.additional_info.recording_mbid",
    "$.track_metadata.additional_info.lastfm_track_mbid",
)
ARTIST_PATHS = (
    "$.track_metadata.mbid_mapping.artist_mbids",
    "$.track_metadata.additional_info.artist_mbids",
    "$.track_metadata.additional_info.lastfm_artist_mbid",
)

PROVENANCE = (
    ("user_id", ("$.user_id",), "그대로"),
    ("user_name", ("$.user_name",), "그대로. 비면 불량으로 격리"),
    ("recording_msid", ("$.recording_msid",), "그대로. 중복 판정 키의 일부"),
    ("track_name", ("$.track_metadata.track_name",), "그대로"),
    ("artist_name", ("$.track_metadata.artist_name",), "그대로"),
    ("release_name", ("$.track_metadata.release_name",), "그대로"),
    ("client_name", ("$.track_metadata.additional_info.submission_client",), "없으면 UNKNOWN"),
    ("duration_ms", ("$.track_metadata.additional_info.duration_ms",), "숫자로 바꿈"),
    ("recording_mbid", RECORDING_PATHS, "위에서부터 처음 있는 UUID. 어느 경로가 쓰였는지가 mbid_source"),
    ("artist_mbids", ARTIST_PATHS, "recording을 고른 같은 경로의 배열. 형식이 틀린 값은 뺌"),
    ("release_mbid", (
        "$.track_metadata.additional_info.release_mbid",
        "$.track_metadata.additional_info.lastfm_release_mbid",
    ), "위에서부터 처음 있는 UUID"),
)

QUALITY_PATHS = ("$.track_metadata",)

UNCHECKED_AGAINST_BUILD = frozenset({"user_name"})


def json_path_value(document, path):
    """`$.a.b.c` 형태의 경로로 값을 꺼낸다. 없거나 객체가 아닌 곳을 지나면 None."""
    if not path.startswith("$."):
        raise ValueError(f"지원하지 않는 경로다: {path}")
    current = document
    for key in path[2:].split("."):
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current
