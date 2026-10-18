import json
from contextlib import contextmanager

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "user_recording_counts"
KEY = ("user_id", "recording_mbid")
METADATA_KEY = "spark.databricks.delta.commitInfo.userMetadata"


class FullRebuildNeeded(RuntimeError):
    pass


def _state(spark: SparkSession, path: str) -> dict | None:
    from delta.tables import DeltaTable

    raw = DeltaTable.forPath(spark, path).history(1).first()["userMetadata"]
    try:
        state = json.loads(raw)[TABLE_NAME]
        if (isinstance(state["source_id"], str)
                and type(state["version"]) is int and state["version"] >= 0):
            return state
    except (ValueError, TypeError, KeyError):
        pass
    return None


def processed_version(spark: SparkSession, target_path: str) -> int | None:
    state = _state(spark, target_path)
    return state["version"] if state else None


@contextmanager
def _commit_metadata(spark, text):
    previous = spark.conf.get(METADATA_KEY, None)
    spark.conf.set(METADATA_KEY, text)
    try:
        yield
    finally:
        if previous is None:
            spark.conf.unset(METADATA_KEY)
        else:
            spark.conf.set(METADATA_KEY, previous)


def aggregate(listens: DataFrame) -> DataFrame:
    return (listens.filter(F.col("recording_mbid").isNotNull())
            .groupBy(*KEY).agg(F.count(F.lit(1)).alias("play_count")))


def apply_changes(spark: SparkSession, deltas: DataFrame, target_path: str,
                  state: dict) -> None:
    from delta.tables import DeltaTable

    text = json.dumps({TABLE_NAME: state})
    target = DeltaTable.forPath(spark, target_path)
    with _commit_metadata(spark, text):
        if deltas.isEmpty():
            escaped_path = target_path.replace("`", "``")
            spark.sql(f"ALTER TABLE delta.`{escaped_path}` SET TBLPROPERTIES "
                      f"('flux.counts.checkpoint' = '{text}')")
            return

        totals = (deltas.join(target.toDF(), list(KEY), "left")
                  .withColumn("total", F.coalesce(F.col("play_count"), F.lit(0))
                              + F.col("delta")))
        if not totals.filter(F.col("total") < 0).isEmpty():
            raise FullRebuildNeeded("집계 횟수가 음수가 된다. --rebuild로 대조해야 한다")

        (target.alias("t").merge(deltas.alias("s"),
                                 "t.user_id = s.user_id AND t.recording_mbid = s.recording_mbid")
         .whenMatchedDelete("t.play_count + s.delta = 0")
         .whenMatchedUpdate(set={"play_count": "t.play_count + s.delta"})
         .whenNotMatchedInsert(condition="s.delta > 0", values={
             "user_id": "s.user_id", "recording_mbid": "s.recording_mbid",
             "play_count": "s.delta"})
         .execute())


def refresh(spark: SparkSession, silver_path: str, target_path: str,
            *, rebuild: bool = False) -> int:
    if silver_path.rstrip("/") == target_path.rstrip("/"):
        raise ValueError("상세 기록과 집계는 같은 경로에 저장할 수 없다")

    from delta.tables import DeltaTable

    source = DeltaTable.forPath(spark, silver_path)
    detail = source.detail().first()
    version = source.history(1).first()["version"]
    state = {"source_id": detail["id"], "version": version}
    if detail["properties"].get("delta.enableChangeDataFeed") != "true":
        raise FullRebuildNeeded("상세 Silver의 CDF를 켠 뒤 --rebuild로 초기화해야 한다")

    if rebuild or not DeltaTable.isDeltaTable(spark, target_path):
        listens = (spark.read.format("delta").option("versionAsOf", version)
                   .load(silver_path))
        (aggregate(listens).write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true")
         .option("userMetadata", json.dumps({TABLE_NAME: state})).save(target_path))
        return version

    done = _state(spark, target_path)
    if (done is None or done["source_id"] != state["source_id"]
            or done["version"] > version):
        raise FullRebuildNeeded("처리 위치가 없거나 원본이 바뀌었다. --rebuild가 필요하다")
    if done["version"] == version:
        return version

    try:
        feed = (spark.read.format("delta").option("readChangeFeed", "true")
                .option("startingVersion", done["version"] + 1)
                .option("endingVersion", version).load(silver_path))
        kind = F.col("_change_type")
        deltas = (feed.filter(F.col("recording_mbid").isNotNull())
                  .withColumn("delta", F.when(kind.isin("insert", "update_postimage"), 1)
                              .otherwise(-1))
                  .groupBy(*KEY).agg(F.sum("delta").alias("delta"))
                  .filter(F.col("delta") != 0).localCheckpoint())
    except Exception as exc:
        raise FullRebuildNeeded("변경 기록을 읽을 수 없다. --rebuild가 필요하다") from exc
    try:
        apply_changes(spark, deltas, target_path, state)
    finally:
        deltas.unpersist()
    return version
