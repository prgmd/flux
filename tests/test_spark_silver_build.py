"""Silver 정제가 판단 모듈과 같은 규칙을 쓰는가.

셋을 본다.

    with_quality   ↔  quality.judge      무엇을 격리하는가
    normalize      ↔  mbid.resolve       MBID를 어디서 가져오는가
    deduplicate    ↔  dedup.pick         어느 줄을 남기는가

실측에서 격리가 0행이었으므로 격리 규칙은 **실데이터로 확인된 적이 없다.**
여기서 사유별로 하나씩 주입해 확인한다.
"""

import json
from datetime import date, datetime, timezone

import pytest

# -m 필터는 수집이 끝난 뒤에 걸린다. 그래서 여기서 먼저 막아야 한다.
# 이 아래 import가 pyspark를 부르므로, 없는 환경에서는 수집 자체가 실패한다.
pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from pyspark.sql import functions as F

from flux.silver import build, dedup, quality, schema
from flux.silver.mbid import MbidSource

pytestmark = pytest.mark.spark

MBID_A = "1557e001-08ec-405d-a0cf-6d2625e90af5"
MBID_B = "8538e728-ca0b-4321-b7e5-cff6565dd4c0"

INGESTED_AT = datetime(2026, 9, 7, 0, 0, tzinfo=timezone.utc)
INGESTED_EPOCH = int(INGESTED_AT.timestamp())
BUILT_AT = datetime(2026, 9, 15, 0, 0, tzinfo=timezone.utc)

BRONZE_SCHEMA = (
    "_ingested_at timestamp, _source_dump string, _source_file string, "
    "_record_id long, listened_at long, user_name string, raw_json string, "
    "ingested_date date"
)


def _bronze_row(document, *, record_id=0, dump="dump-a", listened_at=None):
    """Bronze 한 행. raw_json은 문서를 그대로 직렬화한 것이다."""
    if listened_at is None:
        listened_at = document.get("listened_at", document.get("timestamp"))
    return (
        INGESTED_AT,
        dump,
        "0.listens",
        record_id,
        listened_at,
        document.get("user_name"),
        json.dumps(document, ensure_ascii=False),
        date(2026, 9, 7),
    )


def _listen(**overrides):
    document = {
        "user_name": "alice",
        "listened_at": 1700000000,
        "recording_msid": "msid-1",
        "track_metadata": {"track_name": "곡", "artist_name": "가수"},
    }
    document.update(overrides)
    return document


# ── 격리 ────────────────────────────────────────────────────

QUARANTINE_CASES = [
    ("사용자 이름 없음", _listen(user_name=None)),
    ("사용자 이름 공백", _listen(user_name="   ")),
    ("청취 시각 없음", _listen(listened_at=None)),
    ("1970 이전", _listen(listened_at=-1)),
    ("적재보다 미래", _listen(listened_at=INGESTED_EPOCH + 86400)),
    # get_json_object는 값이 스칼라여도 그것을 돌려준다. isNull()만 보면
    # 이 셋이 통과해 quality.judge와 갈린다. 처음 이 파일을 쓸 때
    # 이 케이스를 빠뜨려 그 갈림을 놓쳤다.
    ("메타데이터 없음", _listen(track_metadata=None)),
    ("메타데이터가 문자열", _listen(track_metadata="문자열")),
    ("메타데이터가 숫자", _listen(track_metadata=42)),
    ("메타데이터가 배열", _listen(track_metadata=[])),
]


@pytest.mark.parametrize("label,document",
                         QUARANTINE_CASES, ids=[c[0] for c in QUARANTINE_CASES])
def test_격리_사유가_quality_judge와_같다(spark, label, document):
    """같은 문서를 두 구현에 넣어 사유를 대조한다."""
    verdict = quality.judge(document, ingested_at=INGESTED_EPOCH)
    assert verdict.ok is False, f"순수 판단이 통과시켰다: {label}"

    bronze = spark.createDataFrame([_bronze_row(document)], BRONZE_SCHEMA)
    row = build.with_quality(bronze).select("_reason").first()
    assert row["_reason"] == verdict.reason


def test_성한_행은_격리되지_않는다(spark):
    document = _listen()
    assert quality.judge(document, ingested_at=INGESTED_EPOCH).ok

    bronze = spark.createDataFrame([_bronze_row(document)], BRONZE_SCHEMA)
    assert build.with_quality(bronze).select("_reason").first()["_reason"] is None


def test_메타데이터가_비어도_통과한다(spark):
    """규칙은 '없음'이다. 비어 있는 것과 없는 것은 다르다.

    객체 여부를 여는 중괄호로 보므로 `{}`는 통과해야 한다. 여기가 깨지면
    방어가 너무 넓게 걸린 것이다.
    """
    document = _listen(track_metadata={})
    assert quality.judge(document, ingested_at=INGESTED_EPOCH).ok

    bronze = spark.createDataFrame([_bronze_row(document)], BRONZE_SCHEMA)
    assert build.with_quality(bronze).select("_reason").first()["_reason"] is None


def test_격리와_통과의_합이_입력과_같다(spark):
    """사유를 여럿 모으지 않는 이유가 이것이다. 한 행이 여러 사유에 걸리면
    사유별 합계가 행 수와 맞지 않는다."""
    documents = [_listen()] + [d for _, d in QUARANTINE_CASES]
    rows = [_bronze_row(d, record_id=i) for i, d in enumerate(documents)]
    bronze = spark.createDataFrame(rows, BRONZE_SCHEMA)

    judged = build.with_quality(bronze)
    passed = judged.filter(judged["_reason"].isNull()).count()
    rejected = judged.filter(judged["_reason"].isNotNull()).count()
    assert passed + rejected == len(documents)
    assert passed == 1


# ── MBID 해석 ───────────────────────────────────────────────

def test_출처_우선순위가_mbid_resolve와_같다(spark):
    """recording MBID를 고른 자리에서 아티스트도 함께 가져와야 한다.
    자리가 섞이면 recording과 artist가 다른 출처에서 온 값이 된다."""
    document = _listen(track_metadata={
        "track_name": "곡",
        "artist_name": "가수",
        "mbid_mapping": {"recording_mbid": MBID_A, "artist_mbids": [MBID_A]},
        "additional_info": {"recording_mbid": MBID_B, "artist_mbids": [MBID_B]},
    })

    from flux.silver import mbid as mbid_module

    expected = mbid_module.resolve(document)
    assert expected.source is MbidSource.MAPPING

    bronze = spark.createDataFrame([_bronze_row(document)], BRONZE_SCHEMA)
    row = build.normalize(bronze, built_at=BUILT_AT).select(
        "recording_mbid", "artist_mbids", "mbid_source").first()

    assert row["mbid_source"] == expected.source.value
    assert row["recording_mbid"] == expected.recording_mbid
    assert tuple(row["artist_mbids"]) == expected.artist_mbids


def test_recording이_없고_아티스트만_있으면_출처가_NONE이다(spark):
    """mbid_source가 가리키는 것은 recording MBID다."""
    document = _listen(track_metadata={
        "track_name": "곡", "artist_name": "가수",
        "additional_info": {"artist_mbids": [MBID_A]},
    })

    from flux.silver import mbid as mbid_module

    expected = mbid_module.resolve(document)
    assert expected.source is MbidSource.NONE
    assert expected.artist_mbids == (MBID_A,)

    bronze = spark.createDataFrame([_bronze_row(document)], BRONZE_SCHEMA)
    row = build.normalize(bronze, built_at=BUILT_AT).select(
        "recording_mbid", "artist_mbids", "mbid_source").first()
    assert row["mbid_source"] == MbidSource.NONE.value
    assert row["recording_mbid"] is None
    assert tuple(row["artist_mbids"]) == (MBID_A,)


def test_대문자_MBID를_소문자로_맞춘다(spark):
    """차원의 gid는 소문자다. 대문자로 남으면 조인이 조용히 빗나가고
    같은 아티스트가 두 개로 세어진다."""
    document = _listen(track_metadata={
        "track_name": "곡", "artist_name": "가수",
        "mbid_mapping": {"recording_mbid": MBID_A.upper(),
                         "artist_mbids": [MBID_B.upper()]},
    })

    bronze = spark.createDataFrame([_bronze_row(document)], BRONZE_SCHEMA)
    row = build.normalize(bronze, built_at=BUILT_AT).select(
        "recording_mbid", "artist_mbids").first()
    assert row["recording_mbid"] == MBID_A
    assert tuple(row["artist_mbids"]) == (MBID_B,)


def test_깨진_MBID_하나가_나머지를_버리지_않는다(spark):
    document = _listen(track_metadata={
        "track_name": "곡", "artist_name": "가수",
        "mbid_mapping": {"recording_mbid": MBID_A,
                         "artist_mbids": ["", MBID_B]},
    })

    bronze = spark.createDataFrame([_bronze_row(document)], BRONZE_SCHEMA)
    row = build.normalize(bronze, built_at=BUILT_AT).select("artist_mbids").first()
    assert tuple(row["artist_mbids"]) == (MBID_B,)


# ── 중복 제거 ───────────────────────────────────────────────

NORMALIZED_SCHEMA = (
    "user_name string, listened_at long, recording_msid string, "
    "mbid_source string, artist_mbids array<string>, "
    "_source_dump string, _record_id long"
)


def _candidates():
    """같은 청취의 네 줄. 넷이 서로 다른 기준에서 갈린다."""
    return [
        dedup.Candidate(record_id=10, source_dump="dump-a",
                        mbid_source=MbidSource.NONE, artist_mbids=()),
        dedup.Candidate(record_id=11, source_dump="dump-a",
                        mbid_source=MbidSource.LASTFM, artist_mbids=(MBID_A,)),
        dedup.Candidate(record_id=12, source_dump="dump-b",
                        mbid_source=MbidSource.MAPPING, artist_mbids=(MBID_A,)),
        dedup.Candidate(record_id=13, source_dump="dump-a",
                        mbid_source=MbidSource.MAPPING,
                        artist_mbids=(MBID_A, MBID_B)),
    ]


def test_남는_줄이_dedup_pick과_같다(spark):
    """정렬 기준 넷을 Spark와 파이썬이 같은 순서로 보는가."""
    candidates = _candidates()
    rows = [
        ("alice", 1700000000, "msid-1", c.mbid_source.value,
         list(c.artist_mbids), c.source_dump, c.record_id)
        for c in candidates
    ]
    survivors = build.deduplicate(
        spark.createDataFrame(rows, NORMALIZED_SCHEMA)).collect()

    assert len(survivors) == 1
    assert survivors[0]["_record_id"] == dedup.pick(candidates).record_id


def test_입력_순서를_바꿔도_같은_줄이_남는다(spark):
    """정렬이 총 순서가 아니면 파티션 배치에 따라 결과가 달라진다.
    `_record_id`가 그것을 막는다 — `_source_dump`를 먼저 보므로
    (_source_dump, _record_id) 쌍이 유일해서 동점이 남지 않는다."""
    candidates = _candidates()
    expected = dedup.pick(candidates).record_id

    for shift in range(len(candidates)):
        rotated = candidates[shift:] + candidates[:shift]
        rows = [
            ("alice", 1700000000, "msid-1", c.mbid_source.value,
             list(c.artist_mbids), c.source_dump, c.record_id)
            for c in rotated
        ]
        survivors = build.deduplicate(
            spark.createDataFrame(rows, NORMALIZED_SCHEMA)).collect()
        assert len(survivors) == 1
        assert survivors[0]["_record_id"] == expected


def test_다른_청취는_각각_남는다(spark):
    """중복 키는 (user_name, listened_at, recording_msid)다."""
    rows = [
        ("alice", 1700000000, "msid-1", "NONE", None, "dump-a", 1),
        ("alice", 1700000000, "msid-2", "NONE", None, "dump-a", 2),
        ("bob", 1700000000, "msid-1", "NONE", None, "dump-a", 3),
        ("alice", 1700000001, "msid-1", "NONE", None, "dump-a", 4),
    ]
    assert build.deduplicate(
        spark.createDataFrame(rows, NORMALIZED_SCHEMA)).count() == 4


def test_작업_컬럼이_결과에_남지_않는다(spark):
    rows = [("alice", 1700000000, "msid-1", "NONE", None, "dump-a", 1)]
    columns = build.deduplicate(
        spark.createDataFrame(rows, NORMALIZED_SCHEMA)).columns
    for working in schema.WORKING_COLUMNS + ("_rn",):
        assert working not in columns


# ── 시간대 ──────────────────────────────────────────────────

def _month_edge(spark):
    """2026-09-30 23:30:00 UTC. 한국 시간으로는 10월 1일 08:30이다."""
    edge = int(datetime(2026, 10, 1, tzinfo=timezone.utc).timestamp()) - 1800
    return (spark.createDataFrame([(edge,)], "s long")
            .select(F.timestamp_seconds("s").alias("t")))


def test_세션_시간대는_UTC다(spark):
    """지정하지 않으면 JVM 기본값을 따른다. 컨테이너에서는 `Etc/UTC`로 나와서
    우연히 맞았을 뿐이다."""
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"


def test_달_경계의_청취는_UTC_달로_들어간다(spark):
    frame = _month_edge(spark)
    assert frame.select(build.listened_month(F.col("t"))).first()[0] == "2026-09"


def test_시간대가_다르면_같은_청취가_다른_달이_된다(spark):
    """세션을 못 박아야 하는 이유. 같은 순간이 서울 시간으로는 10월이다."""
    frame = _month_edge(spark)
    before = spark.conf.get("spark.sql.session.timeZone")
    spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")
    try:
        assert frame.select(build.listened_month(F.col("t"))).first()[0] == "2026-10"
    finally:
        spark.conf.set("spark.sql.session.timeZone", before)
