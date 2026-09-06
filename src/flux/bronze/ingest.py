"""jsonl을 읽어 Bronze Delta에 적재한다.

원본을 파싱해서 다시 직렬화하지 않는다. 한 줄을 문자열 그대로 `raw_json`에 넣고,
파생 컬럼만 JSON 경로로 꺼낸다. 이렇게 하면 Spark가 값을 재해석하며 생기는
변형(키 순서, 숫자 표기, 유니코드 이스케이프)이 원천적으로 없다.
"""

from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flux.bronze import schema

# 한 줄이 JSON 객체로 읽히는지 판정할 때 쓴다.
# get_json_object는 깨진 JSON에도 null을 주므로, 객체 여부는 따로 본다.
_OBJECT_PROBE = "$"


def build_dataframe(
    spark: SparkSession,
    text_path: str,
    *,
    source_dump: str,
    source_file: str,
    ingested_at: datetime | None = None,
) -> DataFrame:
    """텍스트 파일을 읽어 판정 컬럼이 붙은 데이터프레임을 만든다.

    유효한 행과 격리 대상이 아직 섞여 있다. 나누는 것은 split()이 한다.
    """
    ingested_at = ingested_at or datetime.now(timezone.utc)

    lines = spark.read.text(text_path).select(F.col("value").alias("_raw"))

    trimmed = F.trim(F.col("_raw"))
    probe = F.get_json_object(trimmed, _OBJECT_PROBE)

    return (
        lines
        .withColumn("_trimmed", trimmed)
        .withColumn("_probe", probe)
        .withColumn(
            "_reject_reason",
            F.when(F.length(F.col("_trimmed")) == 0, F.lit("빈 줄"))
             .when(F.col("_probe").isNull(), F.lit("JSON 파싱 실패"))
             .when(~F.col("_trimmed").startswith("{"), F.lit("최상위가 객체가 아니다"))
             .otherwise(F.lit(None)),
        )
        .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
        .withColumn("_source_dump", F.lit(source_dump))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_record_id", F.monotonically_increasing_id())
        .withColumn(schema.PARTITION_COLUMN, F.lit(ingested_at.date()).cast("date"))
    )


def split(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """유효한 행과 격리 대상을 나눈다. 두 데이터프레임의 컬럼은 각 스키마와 같다."""
    valid = (
        df.filter(F.col("_reject_reason").isNull())
        .withColumn(
            "listened_at",
            F.coalesce(
                F.get_json_object(F.col("_trimmed"), "$.listened_at"),
                F.get_json_object(F.col("_trimmed"), "$.timestamp"),
            ).cast("long"),
        )
        .withColumn("user_name", F.get_json_object(F.col("_trimmed"), "$.user_name"))
        .withColumnRenamed("_trimmed", "raw_json")
        .select(*schema.column_names())
    )

    rejected = (
        df.filter(F.col("_reject_reason").isNotNull())
        .withColumnRenamed("_raw", "raw_line")
        .withColumnRenamed("_reject_reason", "error")
        .select(*schema.column_names(schema.QUARANTINE_COLUMNS))
    )
    return valid, rejected


def write(df: DataFrame, path: str, *, source_dump: str, mode: str = "overwrite") -> None:
    """Delta로 쓴다. 같은 덤프를 다시 적재하면 그 덤프의 행만 갈아 끼운다.

    무조건 append하면 재실행이 곧 중복이 된다. replaceWhere로 범위를 좁혀
    덮어쓰면 몇 번을 돌려도 결과가 같다.
    """
    writer = (
        df.write.format("delta")
        .mode(mode)
        .partitionBy(schema.PARTITION_COLUMN)
    )
    if mode == "overwrite":
        escaped = source_dump.replace("'", "''")
        writer = writer.option("replaceWhere", f"_source_dump = '{escaped}'")
    writer.save(path)
