"""MBID를 어느 자리에서 가져왔는지까지 함께 정하는 해석 규칙.

같은 MBID라도 ListenBrainz가 해석해 붙인 값과 Last.fm 임포터가 가져온 값은
출처가 다르다. 그 구분을 기록하지 않으면 나중에 어느 쪽이 변했는지 알 수 없다.

규칙과 그 근거는 docs/mbid_resolution.md에 있다. 여기서는 규칙만 옮긴다.
pyspark를 부르지 않는다. 판단이 Spark를 필요로 하지 않기 때문이다.
"""

import re
from dataclasses import dataclass
from enum import Enum

# UUID 8-4-4-4-12. 버전과 변형 비트는 보지 않는다.
# MusicBrainz는 v4를 쓰지만, 형식만 맞으면 통과시키고 실제 존재 여부는
# dimension 조인이 판정한다. 여기서 더 조이면 멀쩡한 값을 버릴 수 있다.
UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Spark 쪽은 정규식 문자열을 그대로 rlike에 넘기므로 패턴을 따로 공개한다.
# 세 곳에 같은 식을 적어 두면 한 곳만 고쳤을 때 판정이 갈린다.
_UUID = re.compile(UUID_PATTERN)

UNKNOWN_CLIENT = "UNKNOWN"


class MbidSource(str, Enum):
    """MBID를 어디서 가져왔는가. 신뢰도 순위가 아니라 출처 표시다."""

    MAPPING = "MAPPING"
    CLIENT = "CLIENT"
    LASTFM = "LASTFM"
    NONE = "NONE"


@dataclass(frozen=True)
class Resolved:
    recording_mbid: str | None
    artist_mbids: tuple[str, ...]
    source: MbidSource


def is_uuid(value) -> bool:
    """UUID 형식인가. 문자열이 아니면 아니다."""
    return isinstance(value, str) and _UUID.match(value.strip()) is not None


def normalize(value):
    """UUID면 소문자로 정리해 돌려주고, 아니면 None."""
    if not is_uuid(value):
        return None
    return value.strip().lower()


def _uuid_list(value) -> tuple[str, ...]:
    """UUID 목록에서 형식이 맞는 것만 남긴다.

    한 항목이 깨졌다고 나머지를 버리지 않는다. 아티스트가 여럿인 곡에서
    하나가 잘못 들어왔다고 나머지 아티스트를 잃으면 손해가 더 크다.
    """
    if isinstance(value, str):
        value = [value]
    if not isinstance(value, (list, tuple)):
        return ()
    return tuple(v for v in (normalize(item) for item in value) if v)


def _as_dict(value) -> dict:
    """dict가 아니면 빈 dict로 본다.

    Bronze는 판단하지 않고 담기만 하므로 여기까지 어떤 모양이든 올 수 있다.
    타입이 다르다고 죽으면 잡 하나가 레코드 한 건에 멈춘다.
    """
    return value if isinstance(value, dict) else {}


def _get(node, *path):
    """중첩 문서에서 경로를 따라간다. 중간이 dict가 아니면 None."""
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def resolve(document) -> Resolved:
    """레코드 하나의 MBID와 그 출처를 정한다.

    자리마다 UUID 형식을 확인하고, 아니면 없는 것으로 보고 다음 자리로 넘어간다.
    형식이 틀린 값 때문에 레코드 전체를 버리지는 않는다.
    """
    metadata = _as_dict(_get(document, "track_metadata"))
    mapping = _as_dict(metadata.get("mbid_mapping"))
    info = _as_dict(metadata.get("additional_info"))

    candidates = (
        (MbidSource.MAPPING, mapping.get("recording_mbid"),
         mapping.get("artist_mbids")),
        (MbidSource.CLIENT, info.get("recording_mbid"),
         info.get("artist_mbids")),
        (MbidSource.LASTFM, info.get("lastfm_track_mbid"),
         info.get("lastfm_artist_mbid")),
    )

    for source, recording, artists in candidates:
        found = normalize(recording)
        if found is not None:
            return Resolved(found, _uuid_list(artists), source)

    # recording MBID가 없어도 아티스트 MBID는 있을 수 있다.
    # 그때도 출처는 NONE이다. 이 컬럼이 가리키는 것은 recording MBID이기 때문이다.
    for _, _, artists in candidates:
        found = _uuid_list(artists)
        if found:
            return Resolved(None, found, MbidSource.NONE)

    return Resolved(None, (), MbidSource.NONE)


def submission_client(document) -> str:
    """제출 클라이언트. 없으면 UNKNOWN.

    키 조합으로 추론하지 않는다. 96.89%에 필드가 직접 있고,
    나머지는 근거 없는 추측으로 채우는 것보다 모른다고 적는 편이 낫다.
    """
    value = _get(document, "track_metadata", "additional_info", "submission_client")
    if isinstance(value, str) and value.strip():
        return value.strip()
    return UNKNOWN_CLIENT
