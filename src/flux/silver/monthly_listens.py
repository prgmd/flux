import json
from pathlib import Path

from pyspark.sql import functions as F

from flux.silver import build, dedup
from flux.silver.monthly_state import exclusive, separate
from flux.silver.quality import MIN_MS_PLAYED
from flux.silver.user_recording_counts import FullRebuildNeeded, _commit_metadata

TABLE = "monthly_user_recordings"
METADATA = "recording_metadata"
QUARANTINE = "invalid_listens"
KEY = ("user_name", "recording_msid", "listened_month")
VARIANT = ("recording_msid", "track_name", "artist_name", "recording_mbid",
           "artist_mbids", "mbid_source")
RAW_KEY = ("_source_dump", "_record_id")


def state_for(source_id, version):
    return {"source_id": source_id, "version": version, "key": list(KEY),
            "minimum_ms_played": MIN_MS_PLAYED, "policy_version": 1, "timezone": "UTC"}


def checkpoint(spark, path):
    from delta.tables import DeltaTable

    if not DeltaTable.isDeltaTable(spark, str(path)):
        return None
    try:
        raw = DeltaTable.forPath(spark, str(path)).history(1).first()["userMetadata"]
        state = json.loads(raw)["monthly_silver"]
        if type(state["version"]) is not int or state["version"] < 0:
            raise ValueError("version")
        if state != state_for(state["source_id"], state["version"]):
            raise ValueError("policy")
        return state
    except (TypeError, KeyError, ValueError) as exc:
        raise FullRebuildNeeded("월별 표의 체크포인트나 품질 정책을 확인할 수 없다") from exc


def ready(spark, root):
    states = [checkpoint(spark, Path(root) / name) for name in (TABLE, METADATA, QUARANTINE)]
    return states[0] is not None and all(s == states[0] for s in states)


def pin_snapshot(spark, root):
    from delta.tables import DeltaTable

    with exclusive(root):
        if not ready(spark, root):
            raise RuntimeError("월별 Silver의 세 표가 아직 같은 버전으로 완성되지 않았다")
        return {name: DeltaTable.forPath(spark, str(Path(root) / name))
                .history(1).first()["version"] for name in (TABLE, METADATA, QUARANTINE)}


def aggregate(winners):
    invalid = F.col("recording_msid").isNull() | (F.trim("recording_msid") == "")
    if not winners.filter(invalid).isEmpty():
        raise ValueError("MSID 없는 청취를 임의로 합칠 수 없다")
    return winners.groupBy(*KEY).agg(F.count(F.lit(1)).alias("play_count"))


def winners(raw, *, built_at):
    valid = build.with_quality(raw).filter(F.col("_reason").isNull())
    return build.finalize(build.deduplicate(build.normalize(valid, built_at=built_at)))


def snapshot(spark, bronze, version):
    return spark.read.format("delta").option("versionAsOf", version).load(bronze)


def changes(spark, bronze, done, end):
    try:
        return (spark.read.format("delta").option("readChangeFeed", "true")
                .option("startingVersion", done + 1).option("endingVersion", end)
                .load(bronze).localCheckpoint(eager=True))
    except Exception as exc:
        raise FullRebuildNeeded("Bronze 변경 이력이 없다. 원본을 보존하고 별도 재생성해야 한다") from exc


def affected(raw, changed):
    times = changed.select("user_name", "listened_at").distinct()
    narrowed = raw.alias("r").join(times.alias("k"),
        F.col("r.user_name").eqNullSafe(F.col("k.user_name")) &
        F.col("r.listened_at").eqNullSafe(F.col("k.listened_at")), "left_semi")
    keys = changed.withColumn("recording_msid", F.get_json_object("raw_json", "$.recording_msid"))
    keys = keys.select(*dedup.KEY).distinct()
    candidates = narrowed.withColumn("recording_msid", F.get_json_object("raw_json", "$.recording_msid"))
    condition = F.lit(True)
    for key in dedup.KEY:
        condition = condition & F.col(f"r.{key}").eqNullSafe(F.col(f"k.{key}"))
    return candidates.alias("r").join(keys.alias("k"), condition, "left_semi")


def touch(spark, path, state):
    text = json.dumps({"monthly_silver": state})
    with _commit_metadata(spark, text):
        spark.sql(f"ALTER TABLE delta.`{str(path).replace('`', '``')}` "
                  f"SET TBLPROPERTIES ('flux.monthly.checkpoint' = '{text}')")


def create(df, path, state, *, partition=None):
    writer = (df.write.format("delta").mode("errorifexists")
              .option("delta.enableChangeDataFeed", "true")
              .option("userMetadata", json.dumps({"monthly_silver": state})))
    if partition:
        writer = writer.partitionBy(partition)
    writer.save(str(path))


def advance_counts(spark, path, state, before, after):
    from delta.tables import DeltaTable

    delta = (aggregate(after).withColumnRenamed("play_count", "delta")
             .unionByName(aggregate(before).select(*KEY, (-F.col("play_count")).alias("delta"))))
    delta = (delta.groupBy(*KEY).agg(F.sum("delta").alias("delta"))
             .filter("delta <> 0").localCheckpoint(eager=True))
    try:
        if delta.isEmpty():
            touch(spark, path, state)
            return
        target = DeltaTable.forPath(spark, str(path))
        totals = delta.join(target.toDF(), list(KEY), "left").withColumn(
            "total", F.coalesce("play_count", F.lit(0)) + F.col("delta"))
        if not totals.filter("total < 0").isEmpty():
            raise FullRebuildNeeded("월별 집계의 횟수가 음수가 된다")
        with _commit_metadata(spark, json.dumps({"monthly_silver": state})):
            (target.alias("t").merge(delta.alias("s"),
                " AND ".join(f"t.{k} = s.{k}" for k in KEY))
             .whenMatchedDelete("t.play_count + s.delta = 0")
             .whenMatchedUpdate(set={"play_count": "t.play_count + s.delta"})
             .whenNotMatchedInsert(condition="s.delta > 0", values={
                 **{k: f"s.{k}" for k in KEY}, "play_count": "s.delta"}).execute())
    finally:
        delta.unpersist()


def advance_metadata(spark, path, state, after):
    from delta.tables import DeltaTable

    variants = after.select(*VARIANT).distinct()
    if not DeltaTable.isDeltaTable(spark, str(path)):
        create(variants, path, state)
        return
    if variants.isEmpty():
        touch(spark, path, state)
        return
    with _commit_metadata(spark, json.dumps({"monthly_silver": state})):
        (DeltaTable.forPath(spark, str(path)).alias("t").merge(variants.alias("s"),
            " AND ".join(f"t.{k} <=> s.{k}" for k in VARIANT))
         .whenNotMatchedInsertAll().execute())
    if checkpoint(spark, path) != state:
        touch(spark, path, state)


def advance_quarantine(spark, path, state, raw, *, built_at, changed=None):
    from delta.tables import DeltaTable

    if changed is not None:
        keys = changed.select(*RAW_KEY).distinct()
        raw = raw.join(keys, list(RAW_KEY), "left_semi")
    rejected = build.quarantine(build.with_quality(raw).filter("_reason is not null"),
                                built_at=built_at)
    if not DeltaTable.isDeltaTable(spark, str(path)):
        create(rejected, path, state)
        return
    missing = keys.join(rejected.select(*RAW_KEY), list(RAW_KEY), "left_anti")
    removals = missing
    for field in rejected.schema:
        if field.name not in RAW_KEY:
            removals = removals.withColumn(field.name, F.lit(None).cast(field.dataType))
    rows = rejected.withColumn("_remove", F.lit(False)).unionByName(
        removals.select(*rejected.columns).withColumn("_remove", F.lit(True)))
    if rows.isEmpty():
        touch(spark, path, state)
        return
    with _commit_metadata(spark, json.dumps({"monthly_silver": state})):
        (DeltaTable.forPath(spark, str(path)).alias("t").merge(rows.alias("s"),
            " AND ".join(f"t.{k} = s.{k}" for k in RAW_KEY))
         .whenMatchedDelete("s._remove")
         .whenMatchedUpdate(condition="NOT s._remove", set={c: f"s.{c}" for c in rejected.columns})
         .whenNotMatchedInsert(condition="NOT s._remove", values={c: f"s.{c}" for c in rejected.columns})
         .execute())
    if checkpoint(spark, path) != state:
        touch(spark, path, state)


def refresh(spark, bronze, root, *, built_at):
    from delta.tables import DeltaTable

    separate(bronze, root)
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("월 경계는 UTC 세션에서 계산해야 한다")
    with exclusive(root):
        source = DeltaTable.forPath(spark, bronze)
        state = state_for(source.detail().first()["id"], source.history(1).first()["version"])
        raw = snapshot(spark, bronze, state["version"])
        cached = {}
        try:
            for name in (TABLE, METADATA, QUARANTINE):
                path = Path(root) / name
                done = checkpoint(spark, path)
                if done == state:
                    continue
                if done and (done["source_id"] != state["source_id"] or done["version"] > state["version"]):
                    raise FullRebuildNeeded("Bronze 식별자 또는 처리 버전이 달라졌다")
                start = done["version"] if done else None
                if start not in cached:
                    feed = changes(spark, bronze, start, state["version"]) if done else None
                    after = winners(affected(raw, feed) if done else raw, built_at=built_at).localCheckpoint(eager=True)
                    before = (winners(affected(snapshot(spark, bronze, start), feed), built_at=built_at)
                              .localCheckpoint(eager=True)) if done else None
                    cached[start] = (feed, before, after)
                feed, before, after = cached[start]
                if name == TABLE:
                    if done:
                        advance_counts(spark, path, state, before, after)
                    else:
                        create(aggregate(after), path, state, partition="listened_month")
                elif name == METADATA:
                    advance_metadata(spark, path, state, after)
                else:
                    advance_quarantine(spark, path, state, raw, built_at=built_at, changed=feed)
        finally:
            for frames in cached.values():
                for frame in frames:
                    if frame is not None:
                        frame.unpersist()
        if not ready(spark, root):
            raise RuntimeError("보조 표와 월별 집계의 처리 버전이 다르다")
        return state
