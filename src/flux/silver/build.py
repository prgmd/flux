"""Bronze를 읽어 Silver로 정형화한다.

네 조각을 엮는다. 판단은 각 모듈이 이미 갖고 있고 여기서는 Spark 표현으로 옮긴다.

    quality.judge   → 격리할 것을 가른다
    mbid.resolve    → MBID와 그 출처를 정한다
    mbid.submission_client → 제출 클라이언트를 읽는다
    dedup.ORDER     → 중복 중 남길 것을 고른다

순서가 중요하다. **격리를 먼저 하고 중복을 나중에 제거한다.**
반대로 하면 격리될 행이 성한 행을 밀어내고 살아남을 수 있다.
"""

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from flux.silver import dedup, schema
from flux.silver.mbid import UUID_PATTERN, MbidSource

EPOCH_1970 = 0


def _json(path: str):
    return F.get_json_object(F.col("raw_json"), path)


def _uuid(column):
    """UUID 형식이면 소문자로, 아니면 NULL.

    형식이 틀렸다고 레코드를 버리지 않는다. 그 자리만 비운다.
    실측에서 걸린 40건이 전부 빈 문자열이었고 여기서 NULL이 된다.
    """
    trimmed = F.trim(column)
    return F.when(trimmed.rlike(UUID_PATTERN), F.lower(trimmed))


def _uuid_array(column):
    """UUID 목록에서 형식이 맞는 것만 남긴다. 하나가 깨져도 나머지를 살린다."""
    cleaned = F.filter(
        F.transform(column, lambda v: _uuid(v)),
        lambda v: v.isNotNull(),
    )
    return F.when(F.size(cleaned) > 0, cleaned)


def _json_array(path: str):
    """JSON 배열을 문자열 배열로. 값 하나만 온 경우도 배열로 감싼다.

    lastfm_artist_mbid는 목록이 아니라 값 하나다. 두 모양을 같은 컬럼에
    담으려면 여기서 맞춰야 한다.
    """
    raw = _json(path)
    parsed = F.from_json(raw, "array<string>")
    return F.when(parsed.isNotNull(), parsed).otherwise(
        F.when(raw.isNotNull(), F.array(raw))
    )


def with_quality(bronze: DataFrame) -> DataFrame:
    """격리 사유를 붙인다. 성한 행은 NULL.

    quality.judge와 같은 순서로 본다. 먼저 걸리는 사유 하나만 남겨야
    사유별 합계가 행 수와 맞는다.
    """
    from flux.silver.quality import Reason, MIN_MS_PLAYED, PLAYED_NUMBER_PATTERN

    # **없는 것과 객체가 아닌 것을 같이 본다.**
    #
    # quality.judge는 `isinstance(..., dict)`로 판정하는데, get_json_object는
    # 값이 문자열이나 숫자여도 그 스칼라를 돌려준다. isNull()만 보면
    # `"track_metadata": "문자열"`이 통과하고, 그러면 테스트가 보장하는 규칙과
    # 실제로 도는 규칙이 다르다. tests/test_silver_quality.py가
    # 문자열·숫자·배열을 전부 격리 대상으로 고정해 두고 있다.
    #
    # 객체 여부는 여는 중괄호로 본다. flux.bronze.ingest가 한 줄이 JSON 객체인지
    # 볼 때 쓰는 것과 같은 방법이다. `"track_metadata": "{...}"`처럼 객체처럼
    # 생긴 문자열은 통과하는데, 그 한계도 Bronze와 같다.
    metadata = _json("$.track_metadata")
    metadata_missing = metadata.isNull() | ~F.trim(metadata).startswith("{")
    listened = F.col("listened_at")
    ingested = F.unix_timestamp(F.col("_ingested_at"))
    user_name = F.trim(F.coalesce(F.col("user_name"), F.lit("")))
    raw_played = _json("$.track_metadata.additional_info.ms_played")
    ms_played = F.when(raw_played.rlike(PLAYED_NUMBER_PATTERN), raw_played.try_cast("double"))

    return bronze.withColumn(
        "_reason",
        F.when(user_name == "", F.lit(Reason.USER_NAME_MISSING))
         .when(listened.isNull(), F.lit(Reason.LISTENED_AT_MISSING))
         .when(listened < EPOCH_1970, F.lit(Reason.LISTENED_AT_BEFORE_1970))
         .when(listened > ingested, F.lit(Reason.LISTENED_AT_FUTURE))
         .when(metadata_missing, F.lit(Reason.METADATA_MISSING))
         .when(ms_played < MIN_MS_PLAYED, F.lit(Reason.PLAYED_TOO_SHORT))
         .otherwise(F.lit(None)),
    )


def normalize(valid: DataFrame, *, built_at) -> DataFrame:
    """MBID를 해석하고 나머지 값을 뽑는다. 중복 제거는 아직 하지 않는다."""
    mapping_recording = _uuid(_json("$.track_metadata.mbid_mapping.recording_mbid"))
    client_recording = _uuid(_json("$.track_metadata.additional_info.recording_mbid"))
    lastfm_recording = _uuid(
        _json("$.track_metadata.additional_info.lastfm_track_mbid"))

    mapping_artists = _uuid_array(
        _json_array("$.track_metadata.mbid_mapping.artist_mbids"))
    client_artists = _uuid_array(
        _json_array("$.track_metadata.additional_info.artist_mbids"))
    lastfm_artists = _uuid_array(
        _json_array("$.track_metadata.additional_info.lastfm_artist_mbid"))

    source = (
        F.when(mapping_recording.isNotNull(), F.lit(MbidSource.MAPPING.value))
         .when(client_recording.isNotNull(), F.lit(MbidSource.CLIENT.value))
         .when(lastfm_recording.isNotNull(), F.lit(MbidSource.LASTFM.value))
         .otherwise(F.lit(MbidSource.NONE.value))
    )
    recording = F.coalesce(mapping_recording, client_recording, lastfm_recording)

    # 아티스트는 recording을 고른 자리에서 함께 가져온다. 자리가 섞이면
    # recording과 artist가 다른 출처에서 온 값이 되어 대응이 깨진다.
    artists = (
        F.when(mapping_recording.isNotNull(), mapping_artists)
         .when(client_recording.isNotNull(), client_artists)
         .when(lastfm_recording.isNotNull(), lastfm_artists)
         .otherwise(F.coalesce(mapping_artists, client_artists, lastfm_artists))
    )

    release = F.coalesce(
        _uuid(_json("$.track_metadata.additional_info.release_mbid")),
        _uuid(_json("$.track_metadata.additional_info.lastfm_release_mbid")),
    )

    client = F.trim(F.coalesce(
        _json("$.track_metadata.additional_info.submission_client"), F.lit("")))

    listened_at = F.col("listened_at").cast("timestamp")

    return (
        valid
        .withColumn("recording_mbid", recording)
        .withColumn("artist_mbids", artists)
        .withColumn("release_mbid", release)
        .withColumn("mbid_source", source)
        .withColumn("client_name",
                    F.when(client == "", F.lit("UNKNOWN")).otherwise(client))
        .withColumn("user_id", _json("$.user_id"))
        .withColumn("recording_msid", _json("$.recording_msid"))
        .withColumn("track_name", _json("$.track_metadata.track_name"))
        .withColumn("artist_name", _json("$.track_metadata.artist_name"))
        .withColumn("release_name", _json("$.track_metadata.release_name"))
        .withColumn("duration_ms",
                    _json("$.track_metadata.additional_info.duration_ms").cast("long"))
        .withColumn("_listened_ts", listened_at)
        .withColumn("_built_at", F.lit(built_at).cast("timestamp"))
    )


def _rank_column():
    """mbid_source를 정렬용 순위로 바꾼다. dedup.SOURCE_RANK와 같아야 한다."""
    column = F.lit(len(dedup.SOURCE_RANK))
    for source, rank in sorted(dedup.SOURCE_RANK.items(), key=lambda p: p[1]):
        column = F.when(F.col("mbid_source") == source.value, F.lit(rank)) \
                  .otherwise(column)
    return column


def _order_columns():
    """dedup.ORDER를 Spark 정렬 식으로 바꾼다.

    순서와 방향을 여기서 다시 적지 않는다. 두 곳에 적으면 갈라진다.
    """
    return [
        F.col(name).asc() if direction == dedup.ASC else F.col(name).desc()
        for name, direction in dedup.ORDER
    ]


def deduplicate(normalized: DataFrame) -> DataFrame:
    """중복 중 하나만 남긴다. 규칙은 docs/deduplication.md."""
    window = Window.partitionBy(*dedup.KEY).orderBy(*_order_columns())
    return (
        normalized
        .withColumn("_mbid_rank", _rank_column())
        .withColumn("_artist_count",
                    F.coalesce(F.size(F.col("artist_mbids")), F.lit(0)))
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn", *schema.WORKING_COLUMNS)
    )


def listened_month(column):
    """청취 시각을 파티션 값("2026-09")으로 바꾼다. **세션 시간대를 따른다.**

    그래서 세션을 UTC로 못 박는다(flux.common.spark.TIME_ZONE). 같은 순간이라도
    시간대에 따라 다른 달 글자가 나온다.
    """
    return F.date_format(column, "yyyy-MM")


def finalize(deduplicated: DataFrame) -> DataFrame:
    """스키마 컬럼만 남기고 순서를 맞춘다."""
    return (
        deduplicated
        .withColumn("listened_at", F.col("_listened_ts"))
        .withColumn(schema.PARTITION_COLUMN, listened_month(F.col("_listened_ts")))
        .select(*schema.column_names())
    )


def quarantine(rejected: DataFrame, *, built_at) -> DataFrame:
    return (
        rejected
        .withColumn("_built_at", F.lit(built_at).cast("timestamp"))
        .withColumnRenamed("_reason", "reason")
        .withColumn(schema.QUARANTINE_PARTITION_COLUMN,
                    F.lit(built_at.date()).cast("date"))
        .select(*schema.column_names(schema.QUARANTINE_COLUMNS))
    )


USER_BUCKETS = 16


def clustered(silver: DataFrame) -> DataFrame:
    """한 사용자의 한 달 치가 파일 하나에 모이게 나눈다.

    증분은 바뀐 키가 든 파일을 통째로 다시 쓴다. 덤프 사이 중복은 과거 기록을 다시
    가져온 사용자 몇 명에게서 나오는데(덤프당 1~8명), 한 달을 키 해시로 흩어 두면 그
    몇 명의 줄이 그 달의 모든 파일에 퍼진다. 실측에서 무거운 날은 52,000개 중
    40,083개를 건드렸다. (달, 사용자)로 모으면 그날 건드릴 파일이 243개를 넘지 않는다
    (scripts/investigations/measure_dump_overlap.py).

    사용자 이름을 해시해 칸 16개로 나눈다. 사용자마다 파일을 따로 두면 파일이 사용자
    수만큼 생긴다.
    """
    bucket = F.pmod(F.xxhash64(F.col("user_name")), F.lit(USER_BUCKETS))
    return silver.repartition(F.col(schema.PARTITION_COLUMN), bucket)


def _judge(bronze: DataFrame) -> tuple:
    judged = with_quality(bronze)
    return (judged.filter(F.col("_reason").isNull()),
            judged.filter(F.col("_reason").isNotNull()))


def build(spark: SparkSession, bronze_path: str, *, built_at, without=()) -> tuple:
    """Bronze를 읽어 (Silver, 격리) 두 데이터프레임을 만든다.

    `without`에 적은 덤프는 빼고 만든다. 증분을 검증할 때 마지막 며칠을 뺀 바탕을
    만드는 데 쓴다.
    """
    bronze = spark.read.format("delta").load(bronze_path)
    if without:
        bronze = bronze.filter(~F.col("_source_dump").isin(list(without)))
    valid, rejected = _judge(bronze)

    silver = clustered(finalize(deduplicate(normalize(valid, built_at=built_at))))
    return silver, quarantine(rejected, built_at=built_at)


def increment(spark: SparkSession, bronze_path: str, silver_path: str, *,
              dump: str, built_at) -> tuple:
    """덤프 하나를 Silver에 붙일 때 바꿔야 할 줄과 그 덤프의 격리 줄을 만든다.

    dedup.pick_arrival의 Spark 표현이다. 새 덤프의 후보와, 같은 키로 Silver에 남아 있는
    줄만 모아 1등을 다시 고른다. Bronze 전체를 다시 보지 않는다.

    돌려주는 줄은 1등이 새 덤프에서 나온 키뿐이다. 1등이 원래 있던 줄이면 바꿀 것이
    없다. 같은 덤프를 다시 붙이면 번호가 내용의 해시라 같은 줄이 다시 이기고,
    merge가 그 줄을 건드리지 않는다.
    """
    bronze = (spark.read.format("delta").load(bronze_path)
              .filter(F.col("_source_dump") == dump))
    valid, rejected = _judge(bronze)
    arrivals = finalize(normalize(valid, built_at=built_at))

    current = (spark.read.format("delta").load(silver_path)
               .join(arrivals.select(*dedup.KEY), list(dedup.KEY), "left_semi"))
    winners = deduplicate(current.unionByName(arrivals))
    changes = (winners.filter(F.col("_source_dump") == dump)
               .select(*schema.column_names()))
    return changes, quarantine(rejected, built_at=built_at)


MERGE_METRICS = (
    "numSourceRows", "numTargetRowsInserted", "numTargetRowsUpdated",
    "numTargetFilesAdded", "numTargetFilesRemoved",
)


def merge(spark: SparkSession, changes: DataFrame, silver_path: str) -> dict:
    """바꿀 줄을 Silver에 넣는다. 키가 있으면 고치고 없으면 넣는다.

    고치는 조건에 "(덤프, 번호)가 다를 때"를 둔다. 같으면 같은 줄이라 다시 쓸 이유가
    없고, 다시 쓰면 그 줄이 든 파일이 통째로 새로 써진다.

    달을 조건에 넣는 것은 결과가 아니라 비용 때문이다. 키에 청취 시각이 있어 달은
    이미 정해지지만, 달이 조건에 있어야 Delta가 파티션 단위로 짝을 맞춘다.
    """
    from delta.tables import DeltaTable

    on = " AND ".join(
        [f"t.{schema.PARTITION_COLUMN} = s.{schema.PARTITION_COLUMN}"]
        + [f"t.{key} = s.{key}" for key in dedup.KEY])
    target = DeltaTable.forPath(spark, silver_path)
    before = target.history(1).select("version").first()[0]
    (
        target.alias("t")
        .merge(changes.alias("s"), on)
        .whenMatchedUpdateAll(
            condition="t._source_dump <> s._source_dump OR t._record_id <> s._record_id")
        .whenNotMatchedInsertAll()
        .execute()
    )
    last = target.history(1).select("version", "operationMetrics").first()
    if last["version"] == before:
        return {name: 0 for name in MERGE_METRICS}
    return {name: int(last["operationMetrics"].get(name, 0)) for name in MERGE_METRICS}


def enable_change_feed(spark: SparkSession, path: str) -> None:
    """Silver가 바뀐 줄을 따로 남기게 한다(Delta Change Data Feed).

    증분 팩트는 Silver 전체를 다시 읽지 않고 "이번에 빠진 청취와 들어온 청취"만
    읽는다(flux.gold.fact_listens.silver_changes). 그 목록이 이 기록이다. 병합이 줄을
    고칠 때마다 고치기 전과 뒤가 함께 남는다.

    통째로 다시 쓰면 표 설정이 그대로인지 장담하지 않고 매번 다시 켠다. 이미 켜져
    있어도 결과는 같다.
    """
    spark.sql(f"ALTER TABLE delta.`{path}` "
              "SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


def replace_quarantine(rejected: DataFrame, path: str, *, dump: str) -> None:
    """그 덤프의 격리 줄만 갈아 끼운다. 격리는 줄 하나만 보고 정하므로 덤프 단위로 끝난다."""
    escaped = dump.replace("'", "''")
    (
        rejected.write.format("delta")
        .mode("overwrite")
        .partitionBy(schema.QUARANTINE_PARTITION_COLUMN)
        .option("replaceWhere", f"_source_dump = '{escaped}'")
        .save(path)
    )


def write(df: DataFrame, path: str, *, partition: str, mode: str = "overwrite") -> None:
    """Delta로 쓴다. Silver는 Bronze 전체에서 다시 만들므로 통째로 덮어쓴다.

    Bronze의 덤프 단위 replaceWhere와 다른 이유는 중복 제거가 덤프를 걸쳐
    작동하기 때문이다. 한 덤프만 다시 처리하면 다른 덤프의 중복을 못 본다.
    """
    (
        df.write.format("delta")
        .mode(mode)
        .partitionBy(partition)
        .option("overwriteSchema", "true")
        .save(path)
    )
