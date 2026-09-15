"""silver.mb_artist를 gold.dim_artist로 옮기며 이력을 쌓는다.

SCD2는 두 가지 일을 한다 — 바뀐 아티스트의 **옛 줄을 닫고**, 새 값으로
**새 줄을 연다**. Delta의 MERGE는 대상 한 줄을 한 번만 건드리므로 두 일을
한 조건으로 쓸 수 없다.

두 문장으로 나누면 그 사이에서 죽었을 때 반쪽이 남는다. 그래서 **원본을
부풀려 한 문장으로** 처리한다. 방법은 _staged에 있다.

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


def _staged(spark: SparkSession, path: str, prepared: DataFrame) -> DataFrame:
    """MERGE 한 번으로 닫고 열기 위해 원본을 두 줄로 부풀린다.

    Delta의 MERGE는 **대상 한 줄을 한 번만** 건드린다. 그래서 같은 아티스트의
    옛 줄을 닫으면서 새 줄을 넣는 것을 한 조건으로 쓸 수 없다.

    바뀐 아티스트를 두 줄로 만들어 넘기면 된다.

        _merge_key = artist_mbid  →  대상과 만나 옛 줄을 닫는다
        _merge_key = NULL         →  아무와도 안 만나 새 줄로 들어간다

    NULL은 어떤 값과도 같지 않으므로 반드시 "안 만남"이 된다. SQL의 그 성질을
    여기서는 **일부러** 쓴다. 다른 데서는 함정이었던 것이 여기서는 도구다.

    같은 규칙을 호스트에서 검증할 수 있는 형태로 적은 것이 scd2.stage다.
    여기는 그것을 Spark 컬럼 연산으로 옮긴 것이다.
    """
    current = (
        spark.read.format("delta").load(path)
        .filter(F.col(scd2.IS_CURRENT))
        .select(
            F.col(scd2.BUSINESS_KEY).alias("_k"),
            *[F.col(name).alias(f"_cur_{name}") for name in scd2.TRACKED_COLUMNS],
        )
    )

    marked = prepared.join(F.broadcast(current),
                           F.col(scd2.BUSINESS_KEY) == F.col("_k"), "left")
    differs = None
    for name in scd2.TRACKED_COLUMNS:
        one = ~F.col(name).eqNullSafe(F.col(f"_cur_{name}"))
        differs = one if differs is None else (differs | one)

    changed_rows = marked.filter(F.col("_k").isNotNull() & differs).select(*COLUMNS)
    new_rows = marked.filter(F.col("_k").isNull()).select(*COLUMNS)

    # 바뀐 아티스트는 두 번 보낸다. 한 번은 닫으러, 한 번은 열러.
    to_close = changed_rows.withColumn(scd2.MERGE_KEY, F.col(scd2.BUSINESS_KEY))
    to_open = changed_rows.withColumn(scd2.MERGE_KEY, F.lit(None).cast("string"))
    brand_new = new_rows.withColumn(scd2.MERGE_KEY, F.lit(None).cast("string"))

    return to_close.unionByName(to_open).unionByName(brand_new)


def merge(spark: SparkSession, path: str, prepared: DataFrame, *, at: str) -> None:
    """닫기와 열기를 **한 번의 MERGE**로 처리한다.

    두 문장으로 나누면 그 사이에서 죽었을 때 옛 줄은 닫혔는데 새 줄이 없는
    상태가 남는다. 실제로 강제 종료 실험에서 그 상태를 만들었다
    (docs/measurements/2026-09-15-dim-artist-scd2.md).

    Delta의 ACID는 **문장 하나**를 지킨다. 잡 전체를 지키려면 문장이 하나여야 한다.
    """
    staged = _staged(spark, path, prepared)
    table = DeltaTable.forPath(spark, path)
    (
        table.alias("t")
        .merge(staged.alias("s"),
               f"t.{scd2.BUSINESS_KEY} = s.{scd2.MERGE_KEY} "
               f"AND t.{scd2.IS_CURRENT} = true")
        .whenMatchedUpdate(
            set={scd2.VALID_TO: F.lit(at), scd2.IS_CURRENT: F.lit(False)})
        .whenNotMatchedInsert(
            values={name: F.col(f"s.{name}") for name in COLUMNS})
        .execute()
    )


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
