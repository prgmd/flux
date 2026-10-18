import json
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import Window
from pyspark.sql import functions as F

from flux.gold import fact


KEY = ["period", "period_start"]


def build(frame):
    if frame.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Period marts require UTC")
    expanded = frame.select(
        "artist_mbid", "user_id", *fact.LISTEN_KEY,
        F.explode(F.array(*[
            F.struct(F.lit(period).alias("period"),
                     F.to_date(F.date_trunc(period, "listened_at")).alias("period_start"))
            for period in ("day", "week")
        ])).alias("_period"),
    ).select("artist_mbid", "user_id", *fact.LISTEN_KEY, "_period.*")
    listen_key = F.struct(*fact.LISTEN_KEY)
    linked = F.col("artist_mbid").isNotNull()
    coverage = expanded.groupBy(*KEY).agg(
        F.countDistinct(listen_key).alias("period_listens"),
        F.countDistinct(F.when(linked, listen_key)).alias("linked_listens"),
        F.countDistinct("user_id").alias("period_users"),
        F.countDistinct(F.when(linked, F.col("user_id"))).alias("linked_users"),
    ).withColumn("coverage", F.col("linked_listens") / F.col("period_listens"))
    by_user = expanded.filter(linked).groupBy(*KEY, "artist_mbid", "user_id").agg(
        F.count(F.lit(1)).alias("_plays"))
    metrics = by_user.groupBy(*KEY, "artist_mbid").agg(
        F.sum("_plays").alias("listens"),
        F.count(F.lit(1)).alias("listeners"),
        F.max("_plays").alias("top_user_listens"),
    ).withColumn("top_user_share", F.col("top_user_listens") / F.col("listens"))
    order = Window.partitionBy(*KEY).orderBy(
        F.col("listeners").desc(), F.col("listens").desc(), F.col("artist_mbid").asc())
    return (
        metrics.withColumn("rank", F.row_number().over(order))
        .join(coverage, KEY)
        .withColumn("period_end", F.date_add("period_start",
                    F.when(F.col("period") == "day", 1).otherwise(7)))
    )


def refresh(spark, fact_path, target_path, *, fact_version=None):
    def qualified(path):
        value = spark._jvm.org.apache.hadoop.fs.Path(path)
        fs = value.getFileSystem(spark._jsc.hadoopConfiguration())
        return str(fs.makeQualified(value).toUri().normalize()).rstrip("/")

    source, target = qualified(fact_path), qualified(target_path)
    if source == target or source.startswith(target + "/") or target.startswith(source + "/"):
        raise ValueError("Source and target must be separate tables")
    table = DeltaTable.forPath(spark, fact_path)
    version = int(table.history(1).first()["version"]) if fact_version is None else fact_version
    metadata = {"fact_path": source, "fact_id": table.detail().first()["id"],
                "fact_version": version, "timezone": "UTC", "periods": ["day", "week"],
                "ranking": "listeners desc, listens desc, artist_mbid asc",
                "built_at": datetime.now(timezone.utc).isoformat()}
    frame = spark.read.format("delta").option("versionAsOf", version).load(fact_path)
    result = build(frame).withColumn("_built_at", F.lit(metadata["built_at"]).cast("timestamp"))
    (result.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .option("userMetadata", json.dumps(metadata)).partitionBy("period").save(target_path))
    return metadata
