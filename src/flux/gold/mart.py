"""집계 마트. 그리고 명세 6.4의 스큐 실험이 재는 바로 그 작업.

**마트 만들기와 스큐 실험이 같은 잡이다.** 실험을 위해 따로 만든 합성 작업이
아니라, 실제로 쓸 마트를 만드는 데 걸리는 시간을 구성만 바꿔 가며 재는 것이다.

스큐가 나는 자리가 명확하다. `artist_mbid`로 묶으면 인기 아티스트 하나가
수백만 줄을 갖는다. 같은 키는 같은 파티션으로 가므로 그 파티션만 커진다.

pyspark를 부른다. 판단은 flux.gold.skew에 있다.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from flux.gold import fact, scd2, skew

DAILY_ARTIST = "mart_daily_artist"


def _listens(spark: SparkSession, fact_path: str) -> DataFrame:
    """집계에 쓸 팩트. 아티스트를 모르는 줄은 뺀다.

    `artist_mbid`가 NULL인 줄이 60%다. 아티스트별 집계에서 그것들은
    한 아티스트가 아니라 **아티스트가 없는 청취**이므로 묶을 대상이 아니다.
    빼지 않으면 NULL 하나가 9천만 줄짜리 최대 파티션이 되어, 재려는 스큐가
    실제 인기 편중이 아니라 결측 때문이 된다.
    """
    return (
        spark.read.format("delta").load(fact_path)
        .filter(F.col("artist_mbid").isNotNull())
        .select("artist_mbid", "listened_at", *fact.LISTEN_KEY)
    )


def _by_day(listens: DataFrame) -> DataFrame:
    return listens.withColumn(
        "listened_date", F.to_date(F.col("listened_at")))


def plain(spark: SparkSession, fact_path: str) -> DataFrame:
    """소금 없이 한 번에 묶는다. 스큐가 그대로 드러나는 경로다."""
    return (
        _by_day(_listens(spark, fact_path))
        .groupBy("artist_mbid", "listened_date")
        .agg(F.count(F.lit(1)).alias("listens"),
             F.countDistinct(*fact.LISTEN_KEY).alias("distinct_listens"))
    )


def salted(spark: SparkSession, fact_path: str) -> DataFrame:
    """소금을 쳐서 두 번에 나눠 묶는다.

    1단계는 키에 임의의 번호를 붙여 큰 키를 SALT_BUCKETS 조각으로 쪼갠다.
    한 태스크가 짊어지던 일이 그만큼 나뉜다.

    2단계는 소금을 떼고 부분합을 더한다. 입력이 이미 1단계에서 줄어 있으므로
    여기서는 스큐가 있어도 다루는 양이 작다.

    **`countDistinct`는 이 방식으로 나눌 수 없다.** 부분끼리 더하면 겹치는 것을
    두 번 세기 때문이다. 그래서 1단계에서 청취 키를 집합으로 모으고 2단계에서
    합집합의 크기를 센다. 합계와 달리 개수 세기는 쪼개기가 공짜가 아니라는
    것이 여기서 드러난다.
    """
    listen_key = F.struct(*[F.col(c) for c in fact.LISTEN_KEY])
    salt = (F.rand() * skew.SALT_BUCKETS).cast("int")

    partial = (
        _by_day(_listens(spark, fact_path))
        .withColumn("_salted",
                    F.concat(F.col("artist_mbid"),
                             F.lit(skew.SALT_SEPARATOR), salt))
        .groupBy("_salted", "listened_date")
        .agg(F.count(F.lit(1)).alias("listens"),
             F.collect_set(listen_key).alias("keys"))
    )
    return (
        partial
        .withColumn("artist_mbid",
                    F.substring_index(F.col("_salted"),
                                      skew.SALT_SEPARATOR, 1))
        .groupBy("artist_mbid", "listened_date")
        .agg(F.sum("listens").alias("listens"),
             F.size(F.array_distinct(F.flatten(F.collect_list("keys"))))
              .alias("distinct_listens"))
    )


def with_names(mart: DataFrame, dim: DataFrame, *, broadcast: bool = True):
    """집계 결과에 그날 유효했던 이름을 붙인다. 시점 조인이다.

    집계 **뒤에** 붙인다. 앞에서 붙이면 1.5억 줄에 조인하게 되고, 뒤에서
    붙이면 아티스트-날짜 조합에만 붙는다.
    """
    versions = dim.select(
        F.col(scd2.BUSINESS_KEY).alias("_k"),
        F.col("name").alias("artist_name"),
        F.col(scd2.VALID_FROM).alias("_from"),
        F.col(scd2.VALID_TO).alias("_to"),
    )
    if broadcast:
        versions = F.broadcast(versions)
    at = F.date_format(F.col("listened_date"), "yyyy-MM-dd")
    return (
        mart.join(
            versions,
            (F.col("artist_mbid") == F.col("_k"))
            & (at >= F.col("_from")) & (at < F.col("_to")),
            "left",
        )
        .drop("_k", "_from", "_to")
    )


def build(spark: SparkSession, fact_path: str, *, salting: bool) -> DataFrame:
    return (salted if salting else plain)(spark, fact_path)


def observe_skew(spark: SparkSession, fact_path: str, *, top: int = 10) -> dict:
    """묶기 전에 치우침이 실재하는지 본다.

    **기법을 적용하기 전에 병목이 있는지 확인하는 순서다.** 없는 병목에
    salting을 걸면 소금값만 치르고 얻는 것이 없다.
    """
    counts = (
        _listens(spark, fact_path)
        .groupBy("artist_mbid").count()
        .orderBy(F.col("count").desc())
    )
    head = counts.limit(top).collect()
    stats = counts.agg(
        F.count(F.lit(1)).alias("keys"),
        F.sum("count").alias("total"),
        F.max("count").alias("max"),
        F.expr("percentile_approx(count, 0.5)").alias("median"),
        F.expr("percentile_approx(count, 0.99)").alias("p99"),
    ).first()

    median = stats["median"] or 0
    return {
        "keys": stats["keys"],
        "total": stats["total"],
        "max": stats["max"],
        "median": median,
        "p99": stats["p99"],
        "max_over_median": round(stats["max"] / median, 1) if median else None,
        "top": [{"artist_mbid": r["artist_mbid"], "listens": r["count"]}
                for r in head],
    }
