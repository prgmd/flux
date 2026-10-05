"""사용자 차원을 만든다. 규칙은 flux.gold.dates.current_name이다.

Silver에서 사용자 번호마다 한 줄로 모은다. 3만 8천 행이라 매번 통째로 다시 만든다.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flux.gold import dates


def build(silver: DataFrame, *, built_at) -> DataFrame:
    """번호마다 가장 최근 청취의 이름과 첫, 마지막 청취, 청취 수.

    `max_by`의 기준을 (청취 시각, 이름) 묶음으로 둔다. 같은 시각에 이름이 둘이면
    이름이 기준의 둘째 자리라 사전순으로 뒤의 것이 이긴다. dates.current_name과 같다.
    """
    latest = F.max_by(F.col("user_name"), F.struct(F.col("listened_at"), F.col("user_name")))
    return (
        silver.groupBy("user_id")
        .agg(latest.alias("user_name"),
             F.min("listened_at").alias("first_listened_at"),
             F.max("listened_at").alias("last_listened_at"),
             F.count(F.lit(1)).alias("listens"))
        .withColumn("_built_at", F.lit(built_at).cast("timestamp"))
        .select(*dates.column_names(dates.USER_COLUMNS))
    )


def write(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)


def users_missing(spark: SparkSession, fact_path: str, user_path: str) -> int:
    """팩트에 있는데 사용자 차원에 없는 번호의 수. 0이어야 한다."""
    used = spark.read.format("delta").load(fact_path).select("user_id").distinct()
    known = spark.read.format("delta").load(user_path).select("user_id")
    return used.join(known, "user_id", "left_anti").count()
