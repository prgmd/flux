import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from flux.silver import user_recording_counts as counts

pytestmark = pytest.mark.spark

INPUT_SCHEMA = "user_id string, recording_mbid string, recording_msid string"
A = "1557e001-08ec-405d-a0cf-6d2625e90af5"
B = "8538e728-ca0b-4321-b7e5-cff6565dd4c0"


def values(df):
    return {(r.user_id, r.recording_mbid): r.play_count for r in df.collect()}


def test_사용자와_녹음으로_묶고_MSID가_달라도_횟수를_합친다(spark):
    source = spark.createDataFrame([
        ("1", A, "msid-a"), ("1", A, "msid-b"),
        ("2", A, "msid-a"), ("1", B, "msid-c"),
        ("1", None, "unknown-a"), ("1", None, "unknown-b"),
    ], INPUT_SCHEMA)
    result = counts.aggregate(source)
    assert values(result) == {("1", A): 2, ("2", A): 1, ("1", B): 1}
    assert result.columns == ["user_id", "recording_mbid", "play_count"]
    assert result.schema["play_count"].dataType.simpleString() == "bigint"


def test_MBid가_없으면_빈_집계가_된다(spark):
    source = spark.createDataFrame([("1", None, "unknown")], INPUT_SCHEMA)
    assert counts.aggregate(source).count() == 0


def test_새_청취와_재실행과_곡_수정을_정확히_반영한다(spark, warehouse):
    source_path = warehouse("silver")
    target_path = warehouse("counts")

    def write(rows):
        (spark.createDataFrame(rows, INPUT_SCHEMA).write.format("delta")
         .mode("overwrite").save(source_path))

    def read():
        return values(spark.read.format("delta").load(target_path))

    write([("1", A, "a"), ("1", None, "unknown")])
    version = counts.refresh(spark, source_path, target_path)
    assert version == 0
    assert read() == {("1", A): 1}
    counts.refresh(spark, source_path, target_path)
    assert read() == {("1", A): 1}

    write([("1", A, "a"), ("1", A, "b"), ("2", B, "c")])
    counts.refresh(spark, source_path, target_path)
    assert read() == {("1", A): 2, ("2", B): 1}

    write([("1", B, "a"), ("1", None, "b")])
    counts.refresh(spark, source_path, target_path)
    assert read() == {("1", B): 1}
    assert spark.read.format("delta").load(source_path).count() == 2


def test_원본과_같은_경로에_집계를_쓰지_않는다(spark, warehouse):
    path = warehouse("silver")
    with pytest.raises(ValueError, match="같은 경로"):
        counts.refresh(spark, path, path + "/")
