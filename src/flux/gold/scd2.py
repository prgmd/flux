"""SCD Type 2 — 값이 바뀌면 고치지 않고 새 줄을 쌓는다.

아티스트가 개명하면 옛 이름 줄을 닫고 새 이름 줄을 연다. 그래야 **과거 청취를
당시 이름으로** 조회할 수 있다. 덮어쓰면 "그때 뭐라고 불렸는지"가 사라진다.

명세 5.4가 Delta Lake를 고른 근거가 이 연산이다. 순수 Parquet은 행 단위 갱신이
안 되어 이력 한 건을 닫으려면 파티션 전체를 다시 써야 한다.

여기서는 **무엇이 바뀌면 새 버전인가**만 정한다. 실제 MERGE는 Spark 쪽이 한다.
pyspark를 부르지 않는다.
"""

from dataclasses import dataclass

TABLE_NAME = "dim_artist"

# 행을 가리키는 자연키. 이 값이 같으면 같은 아티스트다.
BUSINESS_KEY = "artist_mbid"

# 이 컬럼이 바뀌면 새 버전을 만든다.
#
# name과 sort_name만 넣었다. 실측에서 이 둘만 100% 차 있고, 나머지는
# country 37.78%, begin_date 20.17%, end_date 5.22%로 대부분 비어 있다
# (measurements/2026-09-15-mb-artist-load.md).
#
# 비어 있던 칸이 나중에 채워지는 것도 "바뀐" 것으로 세면, 이력의 대부분이
# 개명이 아니라 **MusicBrainz 편집자가 빈칸을 채운 기록**이 된다.
# 그건 이 차원이 답하려는 질문이 아니다.
TRACKED_COLUMNS = ("name", "sort_name")

# 이력에 딸리는 컬럼. 값이 아니라 그 값이 언제부터 언제까지 유효한지를 적는다.
VALID_FROM = "valid_from"
VALID_TO = "valid_to"
IS_CURRENT = "is_current"

# 아직 닫히지 않은 이력의 valid_to. NULL 대신 먼 미래를 쓰면 기간 비교가
# `valid_from <= t AND t < valid_to` 한 줄로 끝난다. NULL이면 비교마다
# "또는 valid_to가 NULL"을 붙여야 하고, 한 곳만 빠뜨려도 조용히 틀린다.
FAR_FUTURE = "9999-12-31"


@dataclass(frozen=True)
class Version:
    """이력 한 줄. 비교에 필요한 값만 담는다."""

    key: str
    values: tuple
    valid_from: str = ""
    valid_to: str = FAR_FUTURE
    is_current: bool = True


def tracked(row) -> tuple:
    """추적 대상 컬럼의 값만 순서대로 뽑는다.

    사전이든 객체든 받는다. Spark Row도 사전처럼 읽히므로 같은 함수로 다룬다.
    """
    getter = row.get if hasattr(row, "get") else lambda name: getattr(row, name, None)
    return tuple(getter(name) for name in TRACKED_COLUMNS)


def changed(current, incoming) -> bool:
    """새 버전을 만들어야 하는가.

    추적 대상 컬럼 중 하나라도 다르면 참이다. 나머지 컬럼은 아무리 바뀌어도
    이력을 만들지 않고 현재 줄의 값만 갱신된다(Type 1 취급).

    파이썬의 `!=`는 None끼리를 같다고 본다. 그래서 이름이 계속 없는 아티스트가
    매번 새 버전을 만들지 않는다.

    **Spark는 반대다.** SQL에서 NULL은 NULL과 같지 않으므로 `col_a != col_b`가
    둘 다 NULL일 때 참도 거짓도 아닌 NULL이 된다. MERGE 조건에 그대로 쓰면
    비교가 통째로 어긋나므로 Spark 쪽은 `eqNullSafe`(`<=>`)를 써야 한다.
    """
    return any(a != b for a, b in zip(tracked(current), tracked(incoming)))


def close(version: Version, *, at: str) -> Version:
    """이력 한 줄을 닫는다. 값은 그대로 두고 유효 구간만 끊는다.

    닫는 시각을 밖에서 받는다. 함수 안에서 오늘 날짜를 읽으면 같은 데이터를
    다시 처리할 때 다른 결과가 나온다.
    """
    return Version(
        key=version.key,
        values=version.values,
        valid_from=version.valid_from,
        valid_to=at,
        is_current=False,
    )


def open_new(key: str, values: tuple, *, at: str) -> Version:
    """새 이력 줄을 연다. 닫히지 않은 상태로 시작한다."""
    return Version(key=key, values=values, valid_from=at,
                   valid_to=FAR_FUTURE, is_current=True)
