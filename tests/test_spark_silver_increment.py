"""덤프 하나씩 붙인 Silver와 그것을 따라간 팩트가 전체에서 다시 만든 것과 같은가.

dedup.pick_arrival이 순수 판단으로 고정한 성질을 Spark 표현(build.increment와
build.merge)이 그대로 지키는지 본다. 덤프 셋에 키 넷을 두고 경우를 나눴다.

    K1  a NONE, b LASTFM, c NONE        새 덤프(c)가 진다. 고칠 것이 없다
    K2  a LASTFM, c LASTFM              출처와 아티스트 수가 같아 나중 덤프가 이긴다. 고친다
    K3  c만                             처음 보는 청취. 넣는다
    K4  a만                             새 덤프와 상관없다. 그대로다
    K5  b NONE, c LASTFM                b가 처음 넣고 c가 이긴다. 아티스트가 없던 줄이 바뀐다

c에는 격리될 줄이 하나 있다.
"""

import json
from datetime import date, datetime, timezone

import pytest

pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from pyspark.sql import functions as F

from flux.gold import dim_artist, fact, fact_listens
from flux.silver import build, schema

pytestmark = pytest.mark.spark

MBID_A = "1557e001-08ec-405d-a0cf-6d2625e90af5"
MBID_R = "8538e728-ca0b-4321-b7e5-cff6565dd4c0"

INGESTED_AT = datetime(2026, 9, 7, 0, 0, tzinfo=timezone.utc)
BUILT_AT = datetime(2026, 9, 15, 0, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 9, 16, 0, 0, tzinfo=timezone.utc)

BRONZE_SCHEMA = (
    "_ingested_at timestamp, _source_dump string, _source_file string, "
    "_record_id long, listened_at long, user_name string, raw_json string, "
    "ingested_date date"
)


def _listen(user, at, msid, source=None):
    metadata = {"track_name": "곡", "artist_name": "가수", "additional_info": {}}
    if source == "LASTFM":
        metadata["additional_info"] = {"lastfm_track_mbid": MBID_R,
                                       "lastfm_artist_mbid": MBID_A}
    document = {"user_name": user, "listened_at": at, "recording_msid": msid,
                "track_metadata": metadata}
    return document


def _row(document, *, dump, record_id):
    return (INGESTED_AT, dump, "0.listens", record_id,
            document.get("listened_at"), document.get("user_name"),
            json.dumps(document, ensure_ascii=False), date(2026, 9, 7))


BRONZE = [
    _row(_listen("alice", 1700000001, "msid-1"), dump="dump-a", record_id=1),
    _row(_listen("alice", 1700000002, "msid-2", "LASTFM"), dump="dump-a", record_id=2),
    _row(_listen("bob", 1700000004, "msid-4"), dump="dump-a", record_id=4),
    _row(_listen("alice", 1700000001, "msid-1", "LASTFM"), dump="dump-b", record_id=11),
    _row(_listen("carol", 1700000006, "msid-6"), dump="dump-b", record_id=12),
    _row(_listen("alice", 1700000001, "msid-1"), dump="dump-c", record_id=21),
    _row(_listen("alice", 1700000002, "msid-2", "LASTFM"), dump="dump-c", record_id=22),
    _row(_listen("bob", 1700000003, "msid-3"), dump="dump-c", record_id=23),
    _row(_listen(None, 1700000005, "msid-5"), dump="dump-c", record_id=24),
    _row(_listen("carol", 1700000006, "msid-6", "LASTFM"), dump="dump-c", record_id=25),
]


@pytest.fixture
def tables(spark, warehouse):
    bronze = warehouse("bronze")
    spark.createDataFrame(BRONZE, BRONZE_SCHEMA).write.format("delta").save(bronze)
    return bronze, warehouse("silver"), warehouse("quarantine"), warehouse("full")


def _content(spark, path):
    """실행 시각 말고 전부. 번호도 넣는다. 내용의 해시라 어느 쪽으로 만들어도 같다."""
    columns = [c for c in schema.column_names() if c != "_built_at"]
    return sorted(tuple(r) for r in spark.read.format("delta").load(path)
                  .select(*columns).collect())


def _write_base(spark, bronze, silver, quarantine, without):
    base, rejected = build.build(spark, bronze, built_at=BUILT_AT, without=without)
    build.write(base, silver, partition=schema.PARTITION_COLUMN)
    build.write(rejected, quarantine, partition=schema.QUARANTINE_PARTITION_COLUMN)


def _attach(spark, bronze, silver, quarantine, dump):
    changes, rejected = build.increment(spark, bronze, silver, dump=dump, built_at=LATER)
    metrics = build.merge(spark, changes, silver)
    build.replace_quarantine(rejected, quarantine, dump=dump)
    return metrics


def test_마지막_덤프를_증분으로_붙이면_전체와_같다(spark, tables):
    bronze, silver, quarantine, full = tables
    everything, _ = build.build(spark, bronze, built_at=BUILT_AT)
    build.write(everything, full, partition=schema.PARTITION_COLUMN)

    _write_base(spark, bronze, silver, quarantine, without=["dump-c"])
    metrics = _attach(spark, bronze, silver, quarantine, "dump-c")

    assert _content(spark, silver) == _content(spark, full)
    assert metrics["numSourceRows"] == 3
    assert metrics["numTargetRowsInserted"] == 1
    assert metrics["numTargetRowsUpdated"] == 2


def test_덤프가_순서대로_오지_않아도_전체와_같다(spark, tables):
    """b가 늦게 와서 a의 줄을 이긴다. 백필로 과거 덤프를 나중에 넣는 경우다."""
    bronze, silver, quarantine, full = tables
    everything, _ = build.build(spark, bronze, built_at=BUILT_AT)
    build.write(everything, full, partition=schema.PARTITION_COLUMN)

    _write_base(spark, bronze, silver, quarantine, without=["dump-b"])
    _attach(spark, bronze, silver, quarantine, "dump-b")

    assert _content(spark, silver) == _content(spark, full)


def test_같은_덤프를_다시_붙이면_아무것도_바꾸지_않는다(spark, tables):
    bronze, silver, quarantine, _ = tables
    _write_base(spark, bronze, silver, quarantine, without=["dump-c"])
    _attach(spark, bronze, silver, quarantine, "dump-c")
    once = _content(spark, silver)

    again = _attach(spark, bronze, silver, quarantine, "dump-c")

    assert _content(spark, silver) == once
    assert again["numTargetRowsInserted"] == 0
    assert again["numTargetRowsUpdated"] == 0


def test_격리_줄은_덤프마다_한_번만_남는다(spark, tables):
    bronze, silver, quarantine, _ = tables
    _write_base(spark, bronze, silver, quarantine, without=["dump-c"])
    _attach(spark, bronze, silver, quarantine, "dump-c")
    _attach(spark, bronze, silver, quarantine, "dump-c")

    rejected = spark.read.format("delta").load(quarantine)
    assert rejected.count() == 1
    assert rejected.first()["_record_id"] == 24


def test_한_사용자의_한_달_치는_파일_하나에_모인다(spark, warehouse):
    """중복 제거는 (사용자, 시각, msid)로 나누므로 같은 사용자의 줄이 여러 파일로
    흩어진다. clustered가 그것을 다시 모으는지 본다. 줄이 스무 개면 묶지 않았을 때
    한 파일에 다 들어갈 확률이 2^-19다."""
    bronze = warehouse("bronze")
    rows = [_row(_listen("alice", 1700000000 + i, f"msid-{i}"), dump="dump-a", record_id=i)
            for i in range(20)]
    spark.createDataFrame(rows, BRONZE_SCHEMA).write.format("delta").save(bronze)
    silver, _ = build.build(spark, bronze, built_at=BUILT_AT)
    path = warehouse("silver")
    build.write(silver, path, partition=schema.PARTITION_COLUMN)

    files = (spark.read.format("delta").load(path)
             .withColumn("_file", F.col("_metadata.file_path"))
             .groupBy(schema.PARTITION_COLUMN, "user_name")
             .agg(F.countDistinct("_file").alias("files"))
             .collect())
    assert [r["files"] for r in files] == [1]


DIM_INPUT_SCHEMA = (
    "artist_mbid string, name string, sort_name string, "
    "country string, area_name string, type string, gender string, "
    "begin_date string, end_date string, ended boolean, comment string, "
    "mb_artist_id string"
)


def _dim(spark, warehouse):
    path = warehouse("dim")
    rows = [(MBID_A, "가수", "가수", None, None, None, None, None, None, None, None, "1")]
    artists = spark.createDataFrame(rows, DIM_INPUT_SCHEMA)
    dim_artist.create(dim_artist.prepare(artists, at="2026-09-15", release="r1"), path)
    return path


def _fact_from(spark, silver, path):
    version = fact_listens.table_version(spark, silver)
    rows = spark.read.format("delta").option("versionAsOf", version).load(silver)
    fact_listens.write(fact_listens.prepare(rows, built_at=BUILT_AT), path,
                       silver_version=version)


def _fact_content(spark, path):
    columns = [c for c in fact.column_names() if c != "_built_at"]
    return sorted((tuple(r) for r in spark.read.format("delta").load(path)
                   .select(*columns).collect()), key=repr)


@pytest.fixture
def expected_fact(spark, tables, warehouse):
    bronze, _, _, full = tables
    everything, _ = build.build(spark, bronze, built_at=BUILT_AT)
    build.write(everything, full, partition=schema.PARTITION_COLUMN)
    path = warehouse("fact-full")
    _fact_from(spark, full, path)
    return path


def _base_with_fact(spark, tables, warehouse, without):
    bronze, silver, quarantine, _ = tables
    _write_base(spark, bronze, silver, quarantine, without=without)
    build.enable_change_feed(spark, silver)
    path = warehouse("fact")
    _fact_from(spark, silver, path)
    return path


def test_팩트가_증분을_따라가면_전체에서_만든_것과_같다(spark, tables, warehouse, expected_fact):
    """K2와 K5는 앞 줄을 지우고 c의 줄을 넣는다. K5의 앞 줄은 아티스트가 없어
    artist_mbid가 NULL이다. NULL끼리 짝이 맞아야 지워진다. K3은 넣기만 한다."""
    bronze, silver, quarantine, _ = tables
    dim = _dim(spark, warehouse)
    fact_path = _base_with_fact(spark, tables, warehouse, without=["dump-c"])

    _attach(spark, bronze, silver, quarantine, "dump-c")
    result = fact_listens.increment(spark, silver, fact_path, dim, built_at=LATER)

    assert _fact_content(spark, fact_path) == _fact_content(spark, expected_fact)
    assert result["numTargetRowsDeleted"] == 2
    assert result["numTargetRowsInserted"] == 3


def test_여러_버전을_한꺼번에_따라가도_같다(spark, tables, warehouse, expected_fact):
    """b와 c를 붙인 뒤 팩트를 한 번에 따라간다. K5는 b에서 들어왔다가 c에서 빠진다.
    범위 안에서 들어왔다 빠진 줄이라 팩트에 들어간 적이 없고, 넣으면 안 된다."""
    bronze, silver, quarantine, _ = tables
    dim = _dim(spark, warehouse)
    fact_path = _base_with_fact(spark, tables, warehouse, without=["dump-b", "dump-c"])

    _attach(spark, bronze, silver, quarantine, "dump-b")
    _attach(spark, bronze, silver, quarantine, "dump-c")
    fact_listens.increment(spark, silver, fact_path, dim, built_at=LATER)

    assert _fact_content(spark, fact_path) == _fact_content(spark, expected_fact)


def test_따라간_뒤_다시_돌리면_아무것도_하지_않는다(spark, tables, warehouse):
    bronze, silver, quarantine, _ = tables
    dim = _dim(spark, warehouse)
    fact_path = _base_with_fact(spark, tables, warehouse, without=["dump-c"])
    _attach(spark, bronze, silver, quarantine, "dump-c")
    fact_listens.increment(spark, silver, fact_path, dim, built_at=LATER)
    once = _fact_content(spark, fact_path)

    again = fact_listens.increment(spark, silver, fact_path, dim, built_at=LATER)

    assert again["applied"] is False
    assert _fact_content(spark, fact_path) == once
    assert fact_listens.watermark(spark, fact_path) == fact_listens.table_version(spark, silver)


def test_Silver를_통째로_다시_쓰면_증분을_막는다(spark, tables, warehouse):
    bronze, silver, _, _ = tables
    dim = _dim(spark, warehouse)
    fact_path = _base_with_fact(spark, tables, warehouse, without=["dump-c"])
    everything, _ = build.build(spark, bronze, built_at=LATER)
    build.write(everything, silver, partition=schema.PARTITION_COLUMN)

    with pytest.raises(fact_listens.FullRebuildNeeded):
        fact_listens.increment(spark, silver, fact_path, dim, built_at=LATER)
