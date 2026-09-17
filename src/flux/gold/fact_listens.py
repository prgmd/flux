"""silver.listens_normalized를 gold.fact_listens로 옮긴다.

두 가지를 한다.

    1. 아티스트 배열을 펼친다        한 줄이 청취-아티스트 쌍이 된다
    2. 차원에 없는 아티스트의 자리를 만든다   late-arriving dimension

pyspark를 부르므로 호스트에서 읽히지 않는다. 판단은 flux.gold.fact에 있다.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flux.gold import fact, scd2


def explode_artists(silver: DataFrame) -> DataFrame:
    """아티스트 배열을 펼쳐 한 줄에 하나로 만든다. 판단은 fact.artists_of다.

    **explode_outer를 쓴다. explode가 아니다.**

    explode는 배열이 비었거나 NULL인 행을 결과에서 **없앤다.** 실측에서
    아티스트 MBID가 없는 청취가 60.32%, 9,119만 행이다. explode를 쓰면
    그만큼이 에러 없이 사라지고 청취 수 집계가 40%짜리가 된다
    (docs/measurements/2026-09-15-fact-grain.md).

    explode_outer는 배열이 비면 artist_mbid가 NULL인 줄 하나를 남긴다.

    **펼치기 전에 array_distinct로 배열 안의 중복을 지운다.** Silver는 형식이
    틀린 값만 걸러내고 중복은 남긴다. 같은 MBID가 두 번 든 청취를 그대로
    펼치면 GRAIN_KEY가 같은 줄이 둘 생기고, 그 아티스트의 청취 수가 두 배가 된다.
    `array_distinct(NULL)`은 NULL이므로 빈 배열의 처리는 달라지지 않는다.
    """
    return silver.withColumn(
        "artist_mbid",
        F.explode_outer(F.array_distinct(F.col("artist_mbids"))))


def collapsed_by_distinct(silver: DataFrame):
    """array_distinct가 지운 원소가 몇 개인가를 세는 식.

    실측에 이 숫자가 없다. measure_fact_grain.py는 배열이 몇 개인지만 셌고
    그 **안**의 중복은 보지 않았다. 방어를 넣은 김에 몇 건이었는지 남긴다.

    0이면 방어가 필요 없었다는 기록이 되고, 0이 아니면 그만큼의 줄이
    게이트를 실패시켰을 것이라는 기록이 된다. 어느 쪽이든 알아야 한다.
    """
    size = F.coalesce(F.size(F.col("artist_mbids")), F.lit(0))
    unique = F.coalesce(
        F.size(F.array_distinct(F.col("artist_mbids"))), F.lit(0))
    return F.sum(size - unique)


def prepare(silver: DataFrame, *, built_at) -> DataFrame:
    """Silver 행을 팩트 모양으로 맞춘다."""
    # listened_month는 Silver에서 그대로 온다. 두 층이 같은 이름을 쓰는 것은
    # 우연이 아니라 tests/test_gold_fact.py가 고정하는 약속이다.
    return (
        explode_artists(silver)
        .withColumn("_built_at", F.lit(built_at).cast("timestamp"))
        .select(*fact.column_names())
    )


def missing_artists(prepared: DataFrame, dim: DataFrame) -> DataFrame:
    """팩트에는 있는데 차원에 없는 아티스트.

    NULL은 세지 않는다. 아티스트를 모르는 청취는 "차원에 없는 아티스트"가
    아니라 "아티스트가 없는 청취"다. 자리를 만들 대상이 아니다.
    """
    used = (
        prepared.filter(F.col("artist_mbid").isNotNull())
                .select("artist_mbid").distinct()
    )
    keys = dim.select(F.col(scd2.BUSINESS_KEY).alias("artist_mbid")).distinct()
    return used.join(keys, on="artist_mbid", how="left_anti")


def placeholders(missing: DataFrame, dim: DataFrame) -> DataFrame:
    """차원에 넣을 자리 행을 만든다. 차원과 같은 컬럼 구성이어야 한다.

    이름만 채우고 나머지는 NULL이다. 아는 것이 MBID뿐이기 때문이다.
    valid_from을 먼 과거로 두어 **어떤 시점의 청취든 이 줄에 걸리게** 한다.
    그러지 않으면 자리를 만들어 놓고도 조인이 안 된다.
    """
    columns = []
    for field in dim.schema.fields:
        name = field.name
        if name == scd2.BUSINESS_KEY:
            column = F.col("artist_mbid")
        elif name == "name":
            column = F.lit(fact.PLACEHOLDER_NAME)
        elif name == scd2.VALID_FROM:
            column = F.lit(fact.PLACEHOLDER_VALID_FROM)
        elif name == scd2.VALID_TO:
            column = F.lit(scd2.FAR_FUTURE)
        elif name == scd2.IS_CURRENT:
            column = F.lit(True)
        else:
            column = F.lit(None)
        columns.append(column.cast(field.dataType).alias(name))
    return missing.select(*columns)


def with_artist_name(listens: DataFrame, dim: DataFrame,
                     *, broadcast: bool = True) -> DataFrame:
    """청취 시각에 유효했던 이름을 붙인다. 시점 조인이다.

    **팩트를 만들 때 쓰지 않는다.** 팩트에는 `artist_mbid`만 싣고 이름은
    질의할 때 붙인다. 대리 키를 두지 않기로 한 선택이 그것이다 — 팩트가
    차원 버전에 고정되지 않으므로, 차원을 고치면 질의 결과가 바로 따라온다.

    쓰이는 곳은 둘이다. 집계 마트를 만들 때, 그리고 "과거 청취가 당시
    이름으로 조회되는가"를 확인할 때.

    **동등 조건을 먼저 걸고 범위 조건을 뒤에 붙인다.** 범위 조건만 쓰면
    Spark가 짝을 찾을 방법이 없어 중첩 루프 조인으로 떨어진다. 왼쪽 한 줄마다
    오른쪽 전체를 훑는 방식이라 1억 5천만 x 297만에서는 끝나지 않는다.

    구간은 시작을 포함하고 끝을 제외한다. 판단은 scd2.covers와 같다.
    양끝을 다 포함하면 이름이 바뀐 바로 그 시각의 청취만 두 줄에 걸린다.

    **브로드캐스트는 플래그로 둔다.** 붙는 것은 이력 전체이고, `F.broadcast()`는
    `autoBroadcastJoinThreshold`를 무시하고 강제하므로 표가 자라도 코드가
    조용하다. 기본값은 지금 동작 그대로 두고 S4의 조인 전략 비교에서 잰다.
    청취에 실제로 쓰인 아티스트가 차원의 10.5%뿐이므로, 조인 전에 차원을
    거르는 쪽이 이길 수도 있다(docs/measurements/2026-09-15-fact-grain.md).
    """
    versions = dim.select(
        F.col(scd2.BUSINESS_KEY).alias("_k"),
        F.col("name").alias("artist_name"),
        F.col(scd2.VALID_FROM).alias("_from"),
        F.col(scd2.VALID_TO).alias("_to"),
    )
    listened_date = F.date_format(F.col("listened_at"), "yyyy-MM-dd")
    return (
        listens.join(
            F.broadcast(versions) if broadcast else versions,
            (F.col("artist_mbid") == F.col("_k"))
            & (listened_date >= F.col("_from"))
            & (listened_date < F.col("_to")),
            "left",
        )
        .drop("_k", "_from", "_to")
    )


def write(df: DataFrame, path: str) -> None:
    """Delta로 쓴다. Silver 전체에서 다시 만들므로 통째로 덮어쓴다."""
    (
        df.write.format("delta")
        .mode("overwrite")
        .partitionBy(fact.PARTITION_COLUMN)
        .option("overwriteSchema", "true")
        .save(path)
    )


def invariants(spark: SparkSession, fact_path: str, dim_path: str) -> dict:
    """팩트가 망가졌는지 센다. 위반은 전부 0이어야 한다."""
    f = spark.read.format("delta").load(fact_path)
    dim_keys = spark.read.format("delta").load(dim_path) \
                    .select(scd2.BUSINESS_KEY).distinct()

    orphan = (
        f.filter(F.col("artist_mbid").isNotNull())
         .select("artist_mbid").distinct()
         .join(dim_keys.withColumnRenamed(scd2.BUSINESS_KEY, "artist_mbid"),
               on="artist_mbid", how="left_anti")
         .count()
    )
    null_month = f.filter(F.col(fact.PARTITION_COLUMN).isNull()).count()

    # 한 줄을 가리키는 키가 겹치는가. 청취 키만 보면 안 된다 — 펼친 결과라
    # 아티스트 수만큼 반복되는 것이 정상이다. 유일해야 하는 것은 grain 전체다.
    #
    # 처음엔 NULL만 보는 불변식을 뒀는데 _record_id 하나를 식별자로 쓴 버그를
    # **못 잡았다.** NULL은 하나도 없으면서 부풀림이 16배였다.
    duplicated = f.count() - f.select(*fact.GRAIN_KEY).distinct().count()

    return {
        "artists_not_in_dim": orphan,
        "duplicate_grain_keys": duplicated,
        "rows_without_partition": null_month,
    }
