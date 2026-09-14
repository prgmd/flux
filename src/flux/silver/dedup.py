"""같은 청취가 여러 줄일 때 어느 줄을 남길지 정한다.

규칙과 근거는 docs/deduplication.md에 있다. 여기서는 규칙만 옮긴다.
pyspark를 부르지 않는다. "어느 쪽이 나은가"는 Spark가 필요 없는 판단이다.

키는 명세 6.3을 그대로 쓴다. 실측에서 세 컬럼 모두 빈 값이 0이었고
한 덤프 안에서 중복을 만들지 않았다.

남길 줄을 고르는 순서는 넷이다.

    1. mbid_source 순위      MAPPING > CLIENT > LASTFM > NONE
    2. artist_mbids 개수     많은 쪽
    3. _source_dump          나중 것
    4. _record_id            작은 쪽

3번까지만 두면 같은 입력에 다른 결과가 나올 수 있다. 앞의 셋이 모두 같은
두 줄의 순서는 파티션 배치에 따라 달라지기 때문이다. 4번이 그것을 막는다.
"""

from dataclasses import dataclass, field

from flux.silver.mbid import MbidSource

KEY = ("user_name", "listened_at", "recording_msid")

# 작은 값이 남는다. 이 순위는 값의 의미가 아니라 이 정렬에 한정된 판단이다.
# MAPPING을 앞에 두는 것은 근거가 있고, CLIENT와 LASTFM 사이는 근거가 약하다.
# 그래도 정해 두지 않으면 실행마다 결과가 달라진다.
SOURCE_RANK = {
    MbidSource.MAPPING: 0,
    MbidSource.CLIENT: 1,
    MbidSource.LASTFM: 2,
    MbidSource.NONE: 3,
}

UNKNOWN_RANK = len(SOURCE_RANK)

ASC = "asc"
DESC = "desc"

# Spark의 Window.orderBy를 만들 때 쓰는 명세. 순수 구현과 같은 순서여야 한다.
ORDER = (
    ("_mbid_rank", ASC),
    ("_artist_count", DESC),
    ("_source_dump", DESC),
    ("_record_id", ASC),
)


@dataclass(frozen=True)
class Candidate:
    """중복 줄 하나. 고르는 데 필요한 값만 담는다."""

    record_id: int
    source_dump: str
    mbid_source: MbidSource = MbidSource.NONE
    artist_mbids: tuple[str, ...] = field(default_factory=tuple)


def source_rank(source) -> int:
    """출처의 정렬 순위. 모르는 값은 맨 뒤로 보낸다.

    없는 값이 들어왔다고 예외를 올리면 잡 하나가 레코드 한 건에 멈춘다.
    맨 뒤로 보내면 다른 후보가 있을 때 자연히 밀려난다.
    """
    return SOURCE_RANK.get(source, UNKNOWN_RANK)


def pick(candidates):
    """남길 줄 하나를 고른다.

    파이썬의 정렬이 안정적이라는 성질을 쓴다. 값이 같은 항목의 상대 순서가
    보존되므로, **덜 중요한 기준부터 차례로 정렬하면** 마지막 정렬이 1순위가 되고
    앞선 정렬들이 동점을 가르는 순서로 남는다.

    방향이 섞인 다중 기준을 한 번의 key 함수로 표현하려면 문자열을 뒤집는 것 같은
    잔재주가 필요하다. 그렇게 쓰면 나중에 읽을 수 없다.
    """
    rows = list(candidates)
    if not rows:
        raise ValueError("고를 후보가 없다")

    rows.sort(key=lambda c: c.record_id)
    rows.sort(key=lambda c: c.source_dump, reverse=True)
    rows.sort(key=lambda c: len(c.artist_mbids), reverse=True)
    rows.sort(key=lambda c: source_rank(c.mbid_source))
    return rows[0]


def dropped(candidates):
    """지워질 줄들. 품질 지표로 남기려고 따로 돌려준다.

    무엇을 지웠는지 세어 두지 않으면 나중에 "합쳤어야 했나"를 판단할 근거가 없다.
    """
    rows = list(candidates)
    kept = pick(rows)
    return [c for c in rows if c is not kept]
