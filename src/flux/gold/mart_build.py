"""집계 마트를 만든다. 그리고 명세 6.4의 스큐 실험이 재는 바로 그 작업.

이름 선언은 flux.gold.mart에 있다. 오라클이 Spark 없이 그것만 읽어야 해서다.

**마트 만들기와 스큐 실험이 같은 잡이다.** 실험을 위해 따로 만든 합성 작업이
아니라, 실제로 쓸 마트를 만드는 데 걸리는 시간을 구성만 바꿔 가며 재는 것이다.

스큐가 나는 자리가 명확하다. `artist_mbid`로 묶으면 인기 아티스트 하나가
수백만 줄을 갖는다. 같은 키는 같은 파티션으로 가므로 그 파티션만 커진다.

pyspark를 부른다. 판단은 flux.gold.skew에 있다.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from flux.gold import fact, mart, scd2, skew


def _listens(spark: SparkSession, fact_path: str,
             months: list | None = None) -> DataFrame:
    """집계에 쓸 팩트. 아티스트를 모르는 줄은 뺀다.

    `artist_mbid`가 NULL인 줄이 60%다. 아티스트별 집계에서 그것들은
    한 아티스트가 아니라 **아티스트가 없는 청취**이므로 묶을 대상이 아니다.
    빼지 않으면 NULL 하나가 9천만 줄짜리 최대 파티션이 되어, 재려는 스큐가
    실제 인기 편중이 아니라 결측 때문이 된다.

    `months`는 사전 확인용이다. 비싼 실험을 돌리기 전에 **같은 코드 경로**를
    한두 달치로만 태워 본다. 파티션 컬럼은 `select`에서 빠지므로, 거르려면
    빠지기 전인 여기서 걸어야 파티션 프루닝이 듣는다.
    """
    frame = spark.read.format("delta").load(fact_path)
    if months:
        frame = frame.filter(F.col(fact.PARTITION_COLUMN).isin(months))
    return (
        frame
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


# ── 스큐 실험이 재는 작업 ───────────────────────────────────
#
# 마트를 만드는 groupBy는 셔플 전에 부분 집계로 접히므로 스큐가 셔플에
# 도달하지 않는다(docs/measurements/2026-09-19-physical-plans.md).
#
# 조인은 접을 것이 없다. 같은 키의 행을 한 곳에 모아야 하기 때문이다.
# 팩트 6,060만 행 중 BTS가 300만인데, 그 300만이 한 파티션에 통째로 간다.


def join_plain(spark: SparkSession, fact_path: str, dim: DataFrame,
               months: list | None = None) -> DataFrame:
    """팩트를 차원에 그대로 조인한다. 스큐가 드러나는 경로다.

    브로드캐스트를 강제하지 않는다. 강제하면 셔플이 없어 스큐도 없다.
    실제로 차원이 커서 임계값을 넘으므로 Spark가 셔플 조인을 고른다.
    """
    return (
        _listens(spark, fact_path, months)
        .join(dim.select(scd2.BUSINESS_KEY, "name"),
              on=scd2.BUSINESS_KEY, how="left")
    )


def join_salted(spark: SparkSession, fact_path: str, dim: DataFrame,
                months: list | None = None) -> DataFrame:
    """조인 양쪽에 소금을 친다.

    큰 쪽(팩트)은 임의의 소금 **하나**를 받고, 작은 쪽(차원)은 소금 가짓수만큼
    **전부로 복제**된다. 그래야 큰 쪽이 어떤 소금을 받든 짝이 있다.

    집계와 다른 점이 여기다. 집계는 한쪽만 쪼개고 나중에 더하면 되지만,
    조인은 **양쪽이 같은 소금을 갖고 있어야 만난다.**

    복제 비용은 작은 쪽에만 붙는다. 다만 차원이 297만 행이라 16배면 4,760만이
    되어 팩트 6,060만과 견줄 만해진다. **이 기법이 공짜가 아니라는 것이
    여기서 드러난다.**

    결과의 행 수는 소금 없는 조인과 같아야 한다. 같은 키가 정확히 한 번만
    만나기 때문이다(flux.gold.skew.join_pairs).
    """
    salt = (F.rand() * skew.SALT_BUCKETS).cast("int")
    left = (
        _listens(spark, fact_path, months)
        .withColumn("_sk", F.concat(F.col(scd2.BUSINESS_KEY),
                                    F.lit(skew.SALT_SEPARATOR), salt))
    )
    right = (
        dim.select(scd2.BUSINESS_KEY, "name")
        .withColumn("_salt", F.explode(
            F.sequence(F.lit(0), F.lit(skew.SALT_BUCKETS - 1))))
        .withColumn("_sk", F.concat(F.col(scd2.BUSINESS_KEY),
                                    F.lit(skew.SALT_SEPARATOR),
                                    F.col("_salt")))
        .drop(scd2.BUSINESS_KEY, "_salt")
    )
    return left.join(right, on="_sk", how="left").drop("_sk")


def join_for_experiment(spark: SparkSession, fact_path: str, dim: DataFrame,
                        *, salting: bool,
                        months: list | None = None) -> DataFrame:
    return (join_salted if salting else join_plain)(
        spark, fact_path, dim, months)


def fingerprint(*columns):
    """행 전체를 숫자 하나로 접는 집계. 두 실행이 같은 답을 냈는지 대조할 때 쓴다.

    행마다 해시를 내고 더한다. 더하기는 순서와 무관하므로 셔플 순서가 달라도 같고,
    어느 행 하나의 값이 달라지면 바뀐다.

    **`decimal(38,0)`로 더한다.** 64비트 해시를 `long`으로 더하면 행이 수백만이면
    넘친다. Spark 4는 ANSI 모드가 기본이라 넘치면 오류로 멈춘다. 워커 종료 실험이
    1,479만 행에서 그렇게 죽었다. ANSI가 꺼진 Spark 3이었다면 조용히 감겨서
    그럴듯한 숫자가 나왔을 것이고, 두 실행이 같은 방식으로 감기니 대조도 통과했을
    것이다. 해시 최댓값(약 9.2×10^18)을 1,500만 번 더해도 10^26 남짓이라 38자리
    안에 들어간다.
    """
    return F.sum(F.xxhash64(*columns).cast("decimal(38,0)"))


def rising(daily: DataFrame, calendar: DataFrame, *, built_at,
           top: int = mart.RISING_TOP, minimum: int = mart.MIN_PREVIOUS_LISTENS) -> DataFrame:
    """일별 마트에서 주마다 급상승 아티스트 상위를 뽑는다. 규칙은 mart.rising이다.

    주는 날짜 차원의 `week_start`(ISO 주의 월요일)로 묶는다. 주의 경계를 여기서 다시
    계산하지 않는다. 이름은 그 주의 마지막 청취 날에 유효했던 이름이다.
    """
    weeks = calendar.select(F.col("date").alias("listened_date"), "week_start")
    weekly = (
        daily.join(weeks, "listened_date")
        .groupBy("artist_mbid", "week_start")
        .agg(F.sum("listens").alias("listens"),
             F.max_by("artist_name", "listened_date").alias("artist_name"))
    )
    previous = weekly.select(
        "artist_mbid",
        F.date_add("week_start", 7).alias("week_start"),
        F.col("listens").alias("previous_listens"),
    )
    growth = F.col("listens") / F.col("previous_listens")
    order = Window.partitionBy("week_start").orderBy(
        growth.desc(), F.col("listens").desc(), F.col("artist_mbid").asc())
    return (
        weekly.join(previous, ["artist_mbid", "week_start"])
        .filter(F.col("previous_listens") >= minimum)
        .withColumn("growth", growth)
        .withColumn("rank", F.row_number().over(order))
        .filter(F.col("rank") <= top)
        .withColumn("_built_at", F.lit(built_at).cast("timestamp"))
        .select("week_start", "rank", "artist_mbid", "artist_name", "listens",
                "previous_listens", "growth", "_built_at")
    )
