from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

TABLE_NAME = "user_recording_counts"
KEY = ("user_id", "recording_mbid")


def aggregate(listens: DataFrame) -> DataFrame:
    return (listens.filter(F.col("recording_mbid").isNotNull())
            .groupBy(*KEY).agg(F.count(F.lit(1)).alias("play_count")))


def refresh(spark: SparkSession, silver_path: str, target_path: str) -> int:
    if silver_path.rstrip("/") == target_path.rstrip("/"):
        raise ValueError("상세 기록과 집계는 같은 경로에 저장할 수 없다")

    from delta.tables import DeltaTable

    version = DeltaTable.forPath(spark, silver_path).history(1).first()["version"]
    listens = (spark.read.format("delta").option("versionAsOf", version)
               .load(silver_path))
    (aggregate(listens).write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(target_path))
    return version
