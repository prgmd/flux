"""날짜 차원을 만든다. 규칙은 flux.gold.dates.attributes다.

기간은 팩트에 있는 첫 청취 날부터 마지막 청취 날까지다. 날짜는 UTC로 자른다
(flux.common.spark.TIME_ZONE). 팩트의 파티션 값을 만든 시계와 같아야 한다.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flux.gold import dates


def listened_range(listens: DataFrame) -> tuple:
    """팩트나 Silver의 첫 청취 날과 마지막 청취 날."""
    row = listens.agg(F.min(F.to_date("listened_at")).alias("first"),
                      F.max(F.to_date("listened_at")).alias("last")).first()
    return row["first"], row["last"]


def build(spark: SparkSession, first, last) -> DataFrame:
    """양끝을 포함한 기간의 날마다 한 줄.

    요일은 Spark의 `dayofweek`가 일요일 1, 토요일 7이라 ISO(월요일 1, 일요일 7)로
    돌린다. ISO 주의 해는 `extract(YEAROFWEEK ...)`, 주 번호는 `weekofyear`다. 둘 다
    ISO 규칙을 따른다.
    """
    day = F.col("date")
    iso_day = (F.dayofweek(day) + 5) % 7 + 1
    days = spark.sql(
        f"SELECT explode(sequence(DATE'{first}', DATE'{last}', INTERVAL 1 DAY)) AS date")
    return (
        days
        .withColumn("year", F.year(day))
        .withColumn("month", F.month(day))
        .withColumn("day", F.dayofmonth(day))
        .withColumn("listened_month", F.date_format(day, "yyyy-MM"))
        .withColumn("iso_year", F.expr("extract(YEAROFWEEK FROM date)").cast("int"))
        .withColumn("iso_week", F.weekofyear(day))
        .withColumn("day_of_week", iso_day.cast("int"))
        .withColumn("is_weekend", iso_day >= 6)
        .withColumn("week_start", F.date_sub(day, iso_day - 1))
        .select(*dates.column_names(dates.DATE_COLUMNS))
    )


def write(df: DataFrame, path: str) -> None:
    """작은 정적 표라 통째로 덮어쓴다. 기간이 늘면 다시 만든다."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
