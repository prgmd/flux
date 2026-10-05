"""덤프 하나씩 붙인 Silver가 전체에서 다시 만든 Silver와 같은가.

dedup.pick_arrival이 순수 판단으로 고정한 성질을 Spark 표현(build.increment와
build.merge)이 그대로 지키는지 본다. 덤프 셋에 키 넷을 두고 경우를 나눴다.

    K1  a NONE, b LASTFM, c NONE        새 덤프(c)가 진다. 고칠 것이 없다
    K2  a LASTFM, c LASTFM              출처와 아티스트 수가 같아 나중 덤프가 이긴다. 고친다
    K3  c만                             처음 보는 청취. 넣는다
    K4  a만                             새 덤프와 상관없다. 그대로다

c에는 격리될 줄이 하나 있다.
"""

import json
from datetime import date, datetime, timezone

import pytest

pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from pyspark.sql import functions as F

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
    _row(_listen("alice", 1700000001, "msid-1"), dump="dump-c", record_id=21),
    _row(_listen("alice", 1700000002, "msid-2", "LASTFM"), dump="dump-c", record_id=22),
    _row(_listen("bob", 1700000003, "msid-3"), dump="dump-c", record_id=23),
    _row(_listen(None, 1700000005, "msid-5"), dump="dump-c", record_id=24),
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
    assert metrics["numSourceRows"] == 2
    assert metrics["numTargetRowsInserted"] == 1
    assert metrics["numTargetRowsUpdated"] == 1


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
