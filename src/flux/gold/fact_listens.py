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
    조용하다. S4에서 쟀다(docs/measurements/2026-10-02-broadcast.md). 로컬에서
    시간은 broadcast, shuffle, 차원 선필터가 같았고, broadcast는 저장 메모리를
    948MB 쓴다. 쓰이는 아티스트(차원의 10.5%)만 남겨 거르면 183MB다. 지금 클러스터
    실행기는 통합 메모리가 456MB라 broadcast가 거기 안 들어간다.
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


USER_BUCKETS = 16


def clustered(df: DataFrame) -> DataFrame:
    """한 사용자의 한 달 치가 파일 하나에 모이게 나눈다. Silver와 같은 이유다
    (flux.silver.build.clustered). 증분이 고쳐 쓰는 줄은 과거 기록을 다시 가져온
    사용자 몇 명에게 몰려 있다. 팩트에는 사용자 이름이 없어 사용자 번호로 나눈다."""
    bucket = F.pmod(F.xxhash64(F.col("user_id")), F.lit(USER_BUCKETS))
    return df.repartition(F.col(fact.PARTITION_COLUMN), bucket)


def write(df: DataFrame, path: str, *, silver_version: int | None = None) -> None:
    """Delta로 쓴다. Silver 전체에서 다시 만들므로 통째로 덮어쓴다.

    `silver_version`을 주면 그 버전까지 반영했다는 처리 위치를 같은 커밋에 적는다
    (fact.watermark_text). 증분은 거기서부터 이어 간다.
    """
    writer = (
        clustered(df).write.format("delta")
        .mode("overwrite")
        .partitionBy(fact.PARTITION_COLUMN)
        .option("overwriteSchema", "true")
    )
    if silver_version is not None:
        writer = writer.option("userMetadata", fact.watermark_text(silver_version))
    writer.save(path)


class FullRebuildNeeded(Exception):
    """증분으로 이어 갈 수 없다. 팩트를 Silver 전체에서 다시 만들어야 한다."""


def table_version(spark: SparkSession, path: str) -> int:
    from delta.tables import DeltaTable
    return DeltaTable.forPath(spark, path).history(1).first()["version"]


def watermark(spark: SparkSession, fact_path: str) -> int | None:
    """팩트가 어느 Silver 버전까지 반영했는가. 가장 최근에 적힌 처리 위치를 읽는다."""
    from delta.tables import DeltaTable
    history = (DeltaTable.forPath(spark, fact_path).history()
               .select("version", "userMetadata").orderBy(F.desc("version")).collect())
    for row in history:
        version = fact.parse_watermark(row["userMetadata"])
        if version is not None:
            return version
    return None


CHANGE_COLUMNS = ("_change_type", "_commit_version", "_commit_timestamp")


def silver_changes(spark: SparkSession, silver_path: str, start: int, end: int) -> tuple:
    """Silver 변경 기록에서 (빠진 청취, 들어온 청취)를 꺼낸다.

    병합이 줄을 고치면 변경 기록에 고치기 전(`update_preimage`)과 고친 뒤
    (`update_postimage`)가 둘 다 남는다. 앞엣것이 팩트에서 지울 청취, 뒤엣것과 새로
    넣은 줄(`insert`)이 팩트에 넣을 청취다.

    여러 버전을 한꺼번에 읽으면 범위 안에서 들어왔다가 다시 빠진 청취가 양쪽에 다
    있다. 그 청취는 팩트에 들어간 적이 없으니 양쪽에서 지운다.
    """
    feed = (spark.read.format("delta")
            .option("readChangeFeed", "true")
            .option("startingVersion", start)
            .option("endingVersion", end)
            .load(silver_path))
    kind = F.col("_change_type")
    removed = feed.filter(kind.isin("update_preimage", "delete")).drop(*CHANGE_COLUMNS)
    added = feed.filter(kind.isin("insert", "update_postimage")).drop(*CHANGE_COLUMNS)
    keys = list(fact.LISTEN_KEY)
    return (removed.join(added.select(*keys), keys, "left_anti"),
            added.join(removed.select(*keys), keys, "left_anti"))


MERGE_METRICS = (
    "numSourceRows", "numTargetRowsInserted", "numTargetRowsDeleted",
    "numTargetFilesAdded", "numTargetFilesRemoved",
)


def apply_changes(spark: SparkSession, fact_path: str, dim_path: str,
                  removed: DataFrame, added: DataFrame, *, built_at,
                  silver_to: int) -> dict:
    """빠진 청취의 줄을 지우고 들어온 청취의 줄을 넣는다. 병합 한 번이다.

    지우기와 넣기를 한 커밋에 하고, 처리 위치도 그 커밋에 적는다. 도중에 죽으면 셋 다
    안 된 것이고, 다시 돌리면 같은 범위를 처음부터 한다.

    아티스트가 비어 있는 줄도 짝을 맞춰야 하므로 아티스트는 NULL끼리도 같다고 보는
    비교(`<=>`)로 맞춘다. 그냥 `=`이면 NULL 줄이 지워지지 않고 남는다.

    넣을 줄과 병합의 원본은 한 번 계산해 고정한다. 자리 행 확인과 병합의 두 단계가
    각자 변경 기록부터 다시 읽지 않게 한다(flux.silver.build.increment와 같은 이유).
    """
    from delta.tables import DeltaTable

    gone = prepare(removed, built_at=built_at)
    arriving = prepare(added, built_at=built_at).localCheckpoint()

    dim = spark.read.format("delta").load(dim_path)
    missing = missing_artists(arriving, dim)
    placed = missing.count()
    if placed:
        placeholders(missing, dim).write.format("delta").mode("append").save(dim_path)

    source = (gone.withColumn("_op", F.lit("delete"))
              .unionByName(arriving.withColumn("_op", F.lit("insert")))
              .localCheckpoint())
    on = (f"t.{fact.PARTITION_COLUMN} = s.{fact.PARTITION_COLUMN}"
          " AND t._source_dump = s._source_dump AND t._record_id = s._record_id"
          " AND t.artist_mbid <=> s.artist_mbid")
    target = DeltaTable.forPath(spark, fact_path)
    before = table_version(spark, fact_path)
    key = "spark.databricks.delta.commitInfo.userMetadata"
    spark.conf.set(key, fact.watermark_text(silver_to))
    try:
        (
            target.alias("t")
            .merge(source.alias("s"), on)
            .whenMatchedDelete(condition="s._op = 'delete'")
            .whenNotMatchedInsert(condition="s._op = 'insert'",
                                  values={c: f"s.{c}" for c in fact.column_names()})
            .execute()
        )
    finally:
        spark.conf.unset(key)
    last = target.history(1).select("version", "operationMetrics").first()
    metrics = ({name: 0 for name in MERGE_METRICS} if last["version"] == before
               else {name: int(last["operationMetrics"].get(name, 0)) for name in MERGE_METRICS})
    metrics["placeholdersAdded"] = placed
    return metrics


def increment(spark: SparkSession, silver_path: str, fact_path: str, dim_path: str,
              *, built_at) -> dict:
    """팩트를 Silver의 지금 버전까지 따라가게 한다. 지난 처리 위치 다음부터 읽는다."""
    from delta.tables import DeltaTable

    done = watermark(spark, fact_path)
    if done is None:
        raise FullRebuildNeeded("팩트에 처리 위치가 없다. 전체에서 한 번 만들어야 한다")
    latest = table_version(spark, silver_path)
    if latest <= done:
        return {"silver_from": done, "silver_to": latest, "applied": False}

    operations = [r["operation"] for r in DeltaTable.forPath(spark, silver_path).history()
                  .filter((F.col("version") > done) & (F.col("version") <= latest))
                  .select("operation").collect()]
    blocker = fact.blocking_operation(operations)
    if blocker:
        raise FullRebuildNeeded(f"Silver에 {blocker}가 있다. 팩트를 다시 만들어야 한다")

    removed, added = silver_changes(spark, silver_path, done + 1, latest)
    metrics = apply_changes(spark, fact_path, dim_path, removed, added,
                            built_at=built_at, silver_to=latest)
    return {"silver_from": done, "silver_to": latest, "applied": True, **metrics}


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
