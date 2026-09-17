"""Gold 팩트 `fact_listens`의 스키마와 판단.

**한 줄은 청취 하나가 아니라 "청취와 아티스트의 쌍"이다.** Silver의
`artist_mbids` 배열을 펼친 결과이기 때문이다.

펼치기로 정한 근거는 실측이다(docs/measurements/2026-09-15-fact-grain.md).
아티스트가 2명 이상인 청취가 0.31%뿐이라 부풀림이 +0.42%로 끝난다.

그래서 **줄을 세는 것과 청취를 세는 것이 다르다.**

    SELECT count(*)                        → 151,826,307   청취-아티스트 쌍
    SELECT count(DISTINCT _source_dump, _record_id)
                                           → 151,192,624   청취

청취를 가리키는 것은 **두 컬럼의 쌍**이다. `_record_id` 하나로는 안 된다.

`_record_id`는 `monotonically_increasing_id()`로 매기는데, 그 함수가 주는 것은
**Spark 잡 하나 안에서의 유일함**이다. Bronze는 덤프마다 별개 잡으로 실었으므로
번호가 덤프를 건너 겹친다. 실측에서 151,192,624행에 서로 다른 `_record_id`가
**9,487,346개**뿐이었다(1억 4,170만 건 겹침).

처음에 `_record_id` 하나를 `listen_id`로 썼다가 부풀림이 16배로 나왔다.
`flux/bronze/merge.py`가 "**적재 안에서** 유일"이라고 이미 적어 둔 것을
전역 유일로 잘못 읽은 탓이다.

pyspark를 부르지 않는다. 실제 적재는 flux.gold.fact_listens가 한다.
"""

from dataclasses import dataclass

from flux.silver.schema import Column

TABLE_NAME = "fact_listens"

# 파티션 기준. 명세 5.3은 listened_date였으나 Silver와 같은 listened_at을
# 쓰므로 날짜가 7,877개로 흩어지는 문제가 그대로 온다(docs/partitioning.md).
PARTITION_COLUMN = "listened_month"

# 청취 하나를 가리키는 키. 한 컬럼이 아니라 쌍이다(위 설명 참고).
# 펼치기 때문에 필요하다. 이게 없으면 청취 수를 다시는 정확히 셀 수 없다.
LISTEN_KEY = ("_source_dump", "_record_id")

# 팩트 한 줄을 가리키는 키. 한 줄이 청취-아티스트 쌍이므로 청취 키만으로는
# 유일하지 않다. 아티스트 수만큼 반복되는 것이 정상이다.
GRAIN_KEY = LISTEN_KEY + ("artist_mbid",)

COLUMNS: tuple[Column, ...] = (
    Column("_source_dump", "string", False,
           "이 청취가 온 덤프. _record_id와 함께 청취 하나를 가리킨다"),
    Column("_record_id", "long", False,
           "덤프 안에서의 행 번호. **덤프를 건너면 겹친다**"),
    Column("listened_at", "timestamp", False, "청취 시각"),
    Column(PARTITION_COLUMN, "string", False, "청취 시각의 연월. 파티션 컬럼"),
    Column("user_id", "string", False, "사용자 번호. dim_user FK"),
    Column("recording_mbid", "string", True, "해석된 recording MBID"),
    Column("artist_mbid", "string", True,
           "펼친 결과라 한 줄에 하나. 배열이 비었으면 NULL"),
    Column("release_mbid", "string", True, "해석된 release MBID"),
    Column("mbid_source", "string", False,
           "MBID 출처. MAPPING/CLIENT/LASTFM/NONE"),
    Column("client_name", "string", False, "제출 클라이언트. 없으면 UNKNOWN"),
    Column("duration_ms", "long", True, "재생 길이. 있는 경우만"),
    Column("_built_at", "timestamp", False, "팩트 생성 시각"),
)


def column_names() -> list[str]:
    return [c.name for c in COLUMNS]


# ── 청취 하나가 몇 줄이 되는가 ──────────────────────────────


def artists_of(mbids) -> tuple:
    """이 청취가 팩트에서 몇 줄이 되고, 각 줄의 `artist_mbid`가 무엇인가.

    grain 규칙 전체가 이 함수 하나다. Spark 쪽(flux.gold.fact_listens)이
    `explode_outer(array_distinct(...))`로 같은 것을 한다.

    **배열이 비면 한 줄을 남긴다.** Spark의 `explode`는 그 줄을 없애므로
    아티스트 MBID가 없는 청취 9,119만 행(60.32%)이 에러 없이 사라진다.
    청취 수 집계가 40%짜리가 되고 아무 데서도 걸리지 않는다.
    그래서 `explode_outer`를 쓴다(docs/measurements/2026-09-15-fact-grain.md).

    **같은 MBID가 두 번 들어 있으면 한 번만 남긴다.** Silver의 `artist_mbids`는
    형식이 틀린 값을 걸러내지만 중복은 지우지 않는다(flux.silver.build._uuid_array).
    클라이언트가 같은 아티스트를 두 번 크레딧하면 그대로 배열에 남는다.

    지우지 않으면 두 가지가 동시에 깨진다.

        1. GRAIN_KEY가 완전히 같은 줄이 둘 생겨 불변식이 깨진다
        2. 그 아티스트의 청취 수가 두 배로 세어진다

    2번이 더 나쁘다. 1번은 게이트가 잡지만 2번은 게이트를 지난 뒤의 집계에서만
    보이고, 그때는 어느 아티스트가 부풀었는지 알 방법이 없다.

    Silver에서 지우지 않는 이유는 배열 길이가 중복 제거의 정렬 키이기 때문이다
    (flux.silver.dedup.ORDER의 `_artist_count`). 거기서 지우면 "아티스트 많은
    쪽을 남긴다"의 판정이 바뀌어 이미 측정한 중복 제거 2.90%가 달라진다.
    **grain의 유일성을 요구하는 것은 팩트이므로 팩트에서 지운다.**

    순서는 처음 나온 순서를 지킨다. Spark의 `array_distinct`가 그렇게 동작하고,
    두 구현이 다른 순서를 주면 이 함수로 확인한 것이 그쪽 결과와 다르다.
    """
    if not mbids:
        return (None,)

    seen = []
    for mbid in mbids:
        if mbid not in seen:
            seen.append(mbid)
    return tuple(seen) if seen else (None,)


def fan_out(mbids) -> int:
    """이 청취가 만드는 팩트 줄 수. 아티스트가 없어도 1이다."""
    return len(artists_of(mbids))


# ── late-arriving dimension ─────────────────────────────────
#
# 팩트에는 있는데 차원에 없는 아티스트가 661개다(0.21%, 위 실측).
# 청취는 2005년부터 쌓인 것이고 차원은 릴리스 하나의 스냅샷이라,
# 그 사이에 생겼거나 병합되어 사라진 MBID가 남는다.
#
# 버리면 청취 기록이 사라지고, NULL로 두면 조인에서 조용히 빠진다.
# **모른다는 것과 없다는 것은 다르다.** 그래서 자리만 만들어 둔다.

PLACEHOLDER_NAME = "(알 수 없는 아티스트)"

# placeholder는 언제부터 유효한가. 어떤 청취든 덮어야 하므로 먼 과거로 둔다.
# 청취 기록의 시작이 2005년이고 그 앞에 여유를 둔다.
PLACEHOLDER_VALID_FROM = "1970-01-01"


@dataclass(frozen=True)
class Placeholder:
    """차원에 없는 아티스트의 자리. 이름 말고는 아는 것이 없다."""

    artist_mbid: str
    name: str = PLACEHOLDER_NAME
    valid_from: str = PLACEHOLDER_VALID_FROM


def placeholder(artist_mbid: str) -> Placeholder:
    """차원에 넣을 자리 하나를 만든다.

    이름을 NULL이 아니라 눈에 보이는 문자열로 둔다. NULL이면 집계 결과에
    빈칸으로 나와 "이름이 없는 아티스트"인지 "차원에 없는 아티스트"인지
    구분되지 않는다. **모르는 이유가 보여야 고칠 수 있다.**
    """
    if not artist_mbid:
        raise ValueError("artist_mbid가 비었다")
    return Placeholder(artist_mbid=artist_mbid)


def is_placeholder(name) -> bool:
    """이 차원 행이 아직 채워지지 않은 자리인가.

    다음 릴리스에 그 아티스트가 들어오면 SCD2 병합이 진짜 이름으로 덮는다.
    그때까지 몇 개가 남아 있는지 세는 데 쓴다.
    """
    return name == PLACEHOLDER_NAME
