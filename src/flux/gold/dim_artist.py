"""silver.mb_artist를 gold.dim_artist로 옮기며 이력을 쌓는다.

명세 5.4의 2단계 MERGE다. 한 번에 못 하는 이유가 있다.

    1단계  값이 바뀐 아티스트의 **현재 줄을 닫는다**
    2단계  새 아티스트와 바뀐 아티스트의 **새 줄을 연다**

Delta의 MERGE는 한 대상 행을 한 번만 건드린다. 같은 실행에서 옛 줄을 닫고
새 줄을 넣으려면 두 번 나눠야 한다.

pyspark를 부르므로 호스트에서 읽히지 않는다. 판단은 flux.gold.scd2에 있다.
"""

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flux.gold import scd2

# Silver에서 그대로 옮기는 값들. 이력을 만들지는 않지만 차원에 필요하다.
CARRIED_COLUMNS = (
    "country", "area_name", "type", "gender",
    "begin_date", "end_date", "ended", "comment", "mb_artist_id",
)

COLUMNS = (
    (scd2.BUSINESS_KEY,)
    + scd2.TRACKED_COLUMNS
    + CARRIED_COLUMNS
    + (scd2.VALID_FROM, scd2.VALID_TO, scd2.IS_CURRENT, "_release")
)


def _changed_condition(target: str, source: str):
    """추적 컬럼 중 하나라도 다른가.

    eqNullSafe를 쓴다. SQL에서 NULL != NULL은 참이 아니라 NULL이라
    그냥 `!=`로 쓰면 이름이 없는 아티스트의 비교가 통째로 NULL이 되고,
    MERGE 조건이 거짓처럼 취급되어 변경을 놓친다.
    """
    condition = None
    for name in scd2.TRACKED_COLUMNS:
        differs = ~F.col(f"{target}.{name}").eqNullSafe(F.col(f"{source}.{name}"))
        condition = differs if condition is None else (condition | differs)
    return condition


def prepare(silver: DataFrame, *, at: str, release: str) -> DataFrame:
    """Silver 행을 차원 한 줄의 모양으로 맞춘다. 아직 이력은 없다."""
    return (
        silver
        .withColumn(scd2.VALID_FROM, F.lit(at))
        .withColumn(scd2.VALID_TO, F.lit(scd2.FAR_FUTURE))
        .withColumn(scd2.IS_CURRENT, F.lit(True))
        .withColumn("_release", F.lit(release))
        .select(*COLUMNS)
    )


def create(prepared: DataFrame, path: str) -> None:
    """차원을 처음 만든다. 모든 행이 첫 번째 버전이다."""
    prepared.write.format("delta").mode("overwrite") \
        .option("overwriteSchema", "true").save(path)


def close_changed(spark: SparkSession, path: str, prepared: DataFrame,
                  *, at: str) -> None:
    """1단계. 값이 바뀐 아티스트의 현재 줄을 닫는다.

    값은 건드리지 않는다. 유효 구간의 끝과 현재 표시만 바꾼다.
    """
    table = DeltaTable.forPath(spark, path)
    (
        table.alias("t")
        .merge(prepared.alias("s"),
               f"t.{scd2.BUSINESS_KEY} = s.{scd2.BUSINESS_KEY} "
               f"AND t.{scd2.IS_CURRENT} = true")
        .whenMatchedUpdate(
            condition=_changed_condition("t", "s"),
            set={scd2.VALID_TO: F.lit(at), scd2.IS_CURRENT: F.lit(False)},
        )
        .execute()
    )


def open_new(spark: SparkSession, path: str, prepared: DataFrame) -> None:
    """2단계. 현재 줄이 없는 아티스트의 새 줄을 연다.

    1단계에서 닫힌 아티스트는 현재 줄이 사라졌으므로 여기서 다시 걸린다.
    한 번도 본 적 없는 아티스트도 같은 경로로 들어온다. **신규와 변경을
    따로 다루지 않아도 되는 것**이 2단계 분리의 이점이다.
    """
    table = DeltaTable.forPath(spark, path)
    (
        table.alias("t")
        .merge(prepared.alias("s"),
               f"t.{scd2.BUSINESS_KEY} = s.{scd2.BUSINESS_KEY} "
               f"AND t.{scd2.IS_CURRENT} = true")
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge(spark: SparkSession, path: str, prepared: DataFrame, *, at: str) -> None:
    """두 단계를 순서대로 실행한다. 순서를 바꾸면 안 된다.

    새 줄을 먼저 열면 그 줄이 현재 줄이 되어 1단계가 방금 넣은 줄을 닫는다.
    """
    close_changed(spark, path, prepared, at=at)
    open_new(spark, path, prepared)


def invariants(spark: SparkSession, path: str) -> dict:
    """명세 게이트의 SCD2 불변식 셋을 센다. 위반은 전부 0이어야 한다."""
    dim = spark.read.format("delta").load(path)

    current_per_key = (
        dim.filter(F.col(scd2.IS_CURRENT))
           .groupBy(scd2.BUSINESS_KEY).count()
           .filter(F.col("count") != 1).count()
    )
    no_current = (
        dim.groupBy(scd2.BUSINESS_KEY)
           .agg(F.sum(F.col(scd2.IS_CURRENT).cast("int")).alias("open"))
           .filter(F.col("open") == 0).count()
    )
    bad_range = dim.filter(
        F.col(scd2.VALID_FROM) >= F.col(scd2.VALID_TO)).count()

    return {
        "keys_without_exactly_one_current": current_per_key,
        "keys_with_no_current": no_current,
        "rows_with_invalid_range": bad_range,
    }
