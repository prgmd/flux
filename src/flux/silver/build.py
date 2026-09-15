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
    from flux.silver.quality import Reason

    metadata = _json("$.track_metadata")
    listened = F.col("listened_at")
    ingested = F.unix_timestamp(F.col("_ingested_at"))
    user_name = F.trim(F.coalesce(F.col("user_name"), F.lit("")))

    return bronze.withColumn(
        "_reason",
        F.when(user_name == "", F.lit(Reason.USER_NAME_MISSING))
         .when(listened.isNull(), F.lit(Reason.LISTENED_AT_MISSING))
         .when(listened < EPOCH_1970, F.lit(Reason.LISTENED_AT_BEFORE_1970))
         .when(listened > ingested, F.lit(Reason.LISTENED_AT_FUTURE))
         .when(metadata.isNull(), F.lit(Reason.METADATA_MISSING))
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


def finalize(deduplicated: DataFrame) -> DataFrame:
    """스키마 컬럼만 남기고 순서를 맞춘다."""
    return (
        deduplicated
        .withColumn("listened_at", F.col("_listened_ts"))
        .withColumn(schema.PARTITION_COLUMN,
                    F.date_format(F.col("_listened_ts"), "yyyy-MM"))
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


def build(spark: SparkSession, bronze_path: str, *, built_at) -> tuple:
    """Bronze를 읽어 (Silver, 격리) 두 데이터프레임을 만든다."""
    bronze = spark.read.format("delta").load(bronze_path)
    judged = with_quality(bronze)

    valid = judged.filter(F.col("_reason").isNull())
    rejected = judged.filter(F.col("_reason").isNotNull())

    silver = finalize(deduplicate(normalize(valid, built_at=built_at)))
    return silver, quarantine(rejected, built_at=built_at)


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
