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
         .option("delta.enableChangeDataFeed", "true")
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


def test_초기화_뒤에는_전체_집계_없이_증분과_빈_변경을_처리한다(
        spark, warehouse, monkeypatch):
    from delta.tables import DeltaTable

    source = warehouse("increment-source")
    target = warehouse("increment-counts")
    (spark.createDataFrame([("1", A, "a")], INPUT_SCHEMA).write.format("delta")
     .option("delta.enableChangeDataFeed", "true").save(source))
    counts.refresh(spark, source, target)

    def forbid_full_scan(*args, **kwargs):
        raise AssertionError("증분 실행에서 전체 집계를 호출했다")

    monkeypatch.setattr(counts, "aggregate", forbid_full_scan)
    (spark.createDataFrame([("1", A, "b"), ("2", B, "c")], INPUT_SCHEMA)
     .write.format("delta").mode("append").save(source))
    version = counts.refresh(spark, source, target)
    assert values(spark.read.format("delta").load(target)) == {("1", A): 2, ("2", B): 1}
    counts.refresh(spark, source, target)
    assert values(spark.read.format("delta").load(target)) == {("1", A): 2, ("2", B): 1}

    table = DeltaTable.forPath(spark, source)
    table.update("recording_msid = 'a'", {"recording_mbid": f"'{B}'"})
    counts.refresh(spark, source, target)
    assert values(spark.read.format("delta").load(target)) == {
        ("1", A): 1, ("1", B): 1, ("2", B): 1}
    table.delete("user_id = '2'")
    table.update("recording_msid = 'b'", {"recording_mbid": "NULL"})
    counts.refresh(spark, source, target)
    assert values(spark.read.format("delta").load(target)) == {("1", B): 1}

    (spark.createDataFrame([("3", None, "unknown")], INPUT_SCHEMA)
     .write.format("delta").mode("append").save(source))
    latest = counts.refresh(spark, source, target)
    assert latest > version
    assert counts.processed_version(spark, target) == latest
    counts.refresh(spark, source, target)
    assert values(spark.read.format("delta").load(target)) == {("1", B): 1}


def test_커밋_직후_실패해도_재시도에서_두번_더하지_않는다(
        spark, warehouse, monkeypatch):
    source = warehouse("retry-source")
    target = warehouse("retry-counts")
    (spark.createDataFrame([("1", A, "a")], INPUT_SCHEMA).write.format("delta")
     .option("delta.enableChangeDataFeed", "true").save(source))
    counts.refresh(spark, source, target)
    (spark.createDataFrame([("1", A, "b")], INPUT_SCHEMA)
     .write.format("delta").mode("append").save(source))
    apply = counts.apply_changes

    def fail_after_commit(*args, **kwargs):
        apply(*args, **kwargs)
        raise RuntimeError("커밋 직후 실패")

    monkeypatch.setattr(counts, "apply_changes", fail_after_commit)
    with pytest.raises(RuntimeError, match="커밋 직후"):
        counts.refresh(spark, source, target)
    counts.refresh(spark, source, target)
    assert values(spark.read.format("delta").load(target)) == {("1", A): 2}


def test_다른_원본이나_처리위치_없는_기존_집계는_명시적_재생성이_필요하다(
        spark, warehouse):
    source = warehouse("source-a")
    other = warehouse("source-b")
    target = warehouse("existing-counts")
    for path in (source, other):
        (spark.createDataFrame([("1", A, "a")], INPUT_SCHEMA).write.format("delta")
         .option("delta.enableChangeDataFeed", "true").save(path))
    (spark.createDataFrame([("1", A, 10)], "user_id string, recording_mbid string, play_count long")
     .write.format("delta").save(target))
    with pytest.raises(counts.FullRebuildNeeded):
        counts.refresh(spark, source, target)
    counts.refresh(spark, source, target, rebuild=True)
    assert values(spark.read.format("delta").load(target)) == {("1", A): 1}
    with pytest.raises(counts.FullRebuildNeeded):
        counts.refresh(spark, other, target)
