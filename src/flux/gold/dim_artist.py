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


def _hint(df: DataFrame, broadcast: bool) -> DataFrame:
    """조인 힌트를 붙일지 말지.

    `F.broadcast()`는 `autoBroadcastJoinThreshold`(기본 10MB)를 **무시하고**
    강제한다. 편한 만큼 위험하다 — 표가 자라도 코드가 조용하다.

    지금 붙는 표는 차원의 현재 줄 전체, 2,976,554행이다. 드라이버 8g로
    지금은 통과하지만 아티스트는 계속 늘고 이력 줄도 쌓인다.

    그리고 같은 저장소의 다른 판단과 어긋난다. 팩트 쪽에서는 297만을 크게 보고
    "조인 전에 차원을 거르는 것"을 선택지로 뒀다
    (docs/measurements/2026-09-15-fact-grain.md).

    **기본값은 지금 동작 그대로 둔다.** 이미 이 설정으로 측정한 결과가 있고,
    기본값을 바꾸면 그 결과를 재현할 수 없다. S4의 AQE 실험이 이 플래그를
    끄고 켜서 재고, 그 결과로 기본값을 정한다.
    """
    return F.broadcast(df) if broadcast else df


_CUR_VALID_FROM = f"_cur_{scd2.VALID_FROM}"


def _marked(spark: SparkSession, path: str, prepared: DataFrame,
            *, broadcast: bool):
    """들어온 줄마다 "차원에 이미 있는가, 값이 달라졌는가"를 붙인다.

    `_staged`와 `unclosable`이 같은 조인을 본다. 따로 만들면 한쪽만 고쳤을 때
    거부 판정과 실제 MERGE가 다른 것을 보게 된다.
    """
    current = (
        spark.read.format("delta").load(path)
        .filter(F.col(scd2.IS_CURRENT))
        .select(
            F.col(scd2.BUSINESS_KEY).alias("_k"),
            F.col(scd2.VALID_FROM).alias(_CUR_VALID_FROM),
            *[F.col(name).alias(f"_cur_{name}") for name in scd2.TRACKED_COLUMNS],
        )
    )

    marked = prepared.join(_hint(current, broadcast),
                           F.col(scd2.BUSINESS_KEY) == F.col("_k"), "left")

    differs = None
    for name in scd2.TRACKED_COLUMNS:
        one = ~F.col(name).eqNullSafe(F.col(f"_cur_{name}"))
        differs = one if differs is None else (differs | one)

    return marked, differs


class SameDayReload(RuntimeError):
    """같은 날짜로 다시 싣다가 빈 유효 구간을 만들게 될 때 발생한다."""


def unclosable(spark: SparkSession, path: str, prepared: DataFrame,
               *, at: str, broadcast: bool = True) -> int:
    """`at` 시점에 닫으면 빈 구간이 되는 줄이 몇 개인가.

    현재 줄의 `valid_from`이 이미 `at`이면, 그 줄을 닫는 순간
    `valid_from == valid_to`가 되어 **유효 구간이 0인 줄**이 남는다.
    `invariants`의 `rows_with_invalid_range`가 그것을 위반으로 센다.

    **잡이 자기 게이트를 깨는 모양**이므로 적재 전에 막는다.

    언제 생기는가: 같은 `--at`으로 두 번 돌리면서 그 사이에 MusicBrainz
    릴리스가 바뀐 경우다. 하루에 릴리스 두 개를 싣는 운영에서 실재한다.
    개명 드릴은 `DRILL_DATE`를 적재일과 분리해 두어 여기 걸리지 않는다.

    값을 바꾸지 않은 줄은 세지 않는다. MERGE가 건드리지 않으므로 무해하다.
    판단은 scd2.closable과 같다.
    """
    marked, differs = _marked(spark, path, prepared, broadcast=broadcast)
    return (
        marked
        .filter(F.col("_k").isNotNull() & differs)
        .filter(F.col(_CUR_VALID_FROM) >= F.lit(at))
        .count()
    )


def _staged(spark: SparkSession, path: str, prepared: DataFrame,
            *, broadcast: bool = True) -> DataFrame:
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
    marked, differs = _marked(spark, path, prepared, broadcast=broadcast)

    changed_rows = marked.filter(F.col("_k").isNotNull() & differs).select(*COLUMNS)
    new_rows = marked.filter(F.col("_k").isNull()).select(*COLUMNS)

    # 바뀐 아티스트는 두 번 보낸다. 한 번은 닫으러, 한 번은 열러.
    to_close = changed_rows.withColumn(scd2.MERGE_KEY, F.col(scd2.BUSINESS_KEY))
    to_open = changed_rows.withColumn(scd2.MERGE_KEY, F.lit(None).cast("string"))
    brand_new = new_rows.withColumn(scd2.MERGE_KEY, F.lit(None).cast("string"))

    return to_close.unionByName(to_open).unionByName(brand_new)


def merge(spark: SparkSession, path: str, prepared: DataFrame, *, at: str,
          broadcast: bool = True) -> None:
    """닫기와 열기를 **한 번의 MERGE**로 처리한다.

    두 문장으로 나누면 그 사이에서 죽었을 때 옛 줄은 닫혔는데 새 줄이 없는
    상태가 남는다. 실제로 강제 종료 실험에서 그 상태를 만들었다
    (docs/measurements/2026-09-15-dim-artist-scd2.md).

    Delta의 ACID는 **문장 하나**를 지킨다. 잡 전체를 지키려면 문장이 하나여야 한다.

    쓰기 전에 `unclosable`을 본다. 빈 유효 구간을 만들 상황이면 **아무것도 쓰지
    않고 거부한다.** 쓴 뒤에 불변식으로 잡으면 이미 망가진 차원을 되돌려야 한다.
    """
    blocked = unclosable(spark, path, prepared, at=at, broadcast=broadcast)
    if blocked:
        raise SameDayReload(
            f"{at}에 닫으면 유효 구간이 빈 줄이 {blocked:,}개 생긴다. "
            f"이미 {at}에 열린 줄을 같은 날짜로 닫으려 하고 있다. "
            "--at을 다음 날짜로 주거나, 앞의 적재를 되돌린 뒤 다시 실어야 한다."
        )

    staged = _staged(spark, path, prepared, broadcast=broadcast)
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
