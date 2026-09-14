"""Silver 계층 스키마 정의.

Bronze와 같은 방식으로 선언과 변환을 나눈다. 선언은 Spark 없이 읽고 검증할 수 있고,
to_spark_schema만 pyspark를 필요로 한다.

명세 5.2는 `silver.listens_normalized`가 "MBID 해석 규칙 적용 결과"라고만 하고
컬럼을 나열하지 않는다. 여기서 정한다. 기준은 둘이다.

- Gold 팩트(명세 5.3)가 필요로 하는 값을 전부 담는다
- 그 값이 **어디서 왔는지**를 함께 담는다

뒤엣것이 이 층의 존재 이유다. Bronze는 원본을 그대로 담고, Gold는 집계를 담는다.
Silver는 **원본을 해석한 결과와 그 해석의 근거**를 담는 자리다.
"""

from dataclasses import dataclass

TABLE_NAME = "listens_normalized"
QUARANTINE_TABLE_NAME = "invalid_listens"

# 파티션 기준. 명세 5.3은 listened_date라고 하지만 실측에서 조각의 99.5%가
# 10만 행 미만(중앙값 1.2MB)이 되는 것을 확인해 달 단위로 바꿨다.
# 근거는 docs/partitioning.md.
PARTITION_COLUMN = "listened_month"


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool
    doc: str


COLUMNS: tuple[Column, ...] = (
    Column("listened_at", "timestamp", False, "청취 시각"),
    Column(PARTITION_COLUMN, "string", False, "청취 시각의 연월. 파티션 컬럼"),
    Column("user_id", "string", False, "사용자 번호. dim_user FK. 전수로 안정성 확인함"),
    Column("user_name", "string", False, "사용자 이름. 바뀔 수 있으므로 키로 쓰지 않는다"),
    Column("recording_msid", "string", False,
           "ListenBrainz가 이름 조합에 붙인 식별자. 100% 존재하지만 MusicBrainz와 다른 세계"),
    Column("recording_mbid", "string", True, "해석된 recording MBID"),
    Column("artist_mbids", "array<string>", True, "해석된 아티스트 MBID 목록"),
    Column("release_mbid", "string", True, "해석된 release MBID"),
    Column("mbid_source", "string", False,
           "MBID를 어디서 가져왔는가. MAPPING/CLIENT/LASTFM/NONE. 신뢰도 순위가 아니라 출처"),
    Column("client_name", "string", False, "제출 클라이언트. 없으면 UNKNOWN"),
    Column("track_name", "string", True, "곡 이름"),
    Column("artist_name", "string", True, "아티스트 이름"),
    Column("release_name", "string", True, "앨범 이름"),
    Column("duration_ms", "long", True, "재생 길이. 있는 경우만"),
    Column("_source_dump", "string", False, "이 행이 살아남은 덤프. 중복 제거 결과를 추적한다"),
    Column("_record_id", "long", False, "Bronze의 행 식별자. 여기서 거슬러 올라간다"),
    Column("_ingested_at", "timestamp", False, "Bronze 적재 시각"),
    Column("_built_at", "timestamp", False, "Silver 생성 시각"),
)

# 격리는 원본을 그대로 들고 간다. 판단이 틀렸을 때 되돌릴 수 있어야 하기 때문이다.
QUARANTINE_COLUMNS: tuple[Column, ...] = (
    Column("_source_dump", "string", False, "출처 덤프 식별자"),
    Column("_record_id", "long", False, "Bronze의 행 식별자"),
    Column("_ingested_at", "timestamp", False, "Bronze 적재 시각"),
    Column("_built_at", "timestamp", False, "Silver 생성 시각"),
    Column("reason", "string", False, "격리 사유 한 줄"),
    Column("raw_json", "string", False, "원본 문서 전체"),
    Column("quarantined_date", "date", False, "격리 날짜. 파티션 컬럼"),
)

QUARANTINE_PARTITION_COLUMN = "quarantined_date"

# 중복 제거를 위해 계산해 두는 컬럼. 최종 스키마에는 남지 않는다.
# 이름을 밑줄로 시작하는 것은 결과에 남을 컬럼과 구분하기 위해서다.
WORKING_COLUMNS = ("_mbid_rank", "_artist_count")


def column_names(columns: tuple[Column, ...] = COLUMNS) -> list[str]:
    return [c.name for c in columns]


def ddl(columns: tuple[Column, ...] = COLUMNS) -> str:
    parts = []
    for c in columns:
        suffix = "" if c.nullable else " NOT NULL"
        parts.append(f"{c.name} {c.type}{suffix}")
    return ", ".join(parts)


SPARK_TYPES = (
    "string", "long", "int", "double", "boolean",
    "timestamp", "date", "array<string>",
)


def validate_types(columns: tuple[Column, ...] = COLUMNS) -> None:
    """타입 이름이 아는 것인지 확인한다. pyspark 없이 돈다.

    선언을 Spark 없이 검증할 수 있어야 나눈 의미가 있다. 이 검사가
    to_spark_schema 안에만 있으면 컨테이너 안에서만 돌아 늦게 드러난다.
    """
    for c in columns:
        if c.type not in SPARK_TYPES:
            raise ValueError(f"모르는 타입: {c.type} ({c.name})")


def to_spark_schema(columns: tuple[Column, ...] = COLUMNS):
    """Spark 스키마 객체로 바꾼다. 이 함수만 pyspark를 필요로 한다."""
    validate_types(columns)

    from pyspark.sql import types as T

    mapping = {
        "string": T.StringType(),
        "long": T.LongType(),
        "int": T.IntegerType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "timestamp": T.TimestampType(),
        "date": T.DateType(),
        "array<string>": T.ArrayType(T.StringType()),
    }
    fields = []
    for c in columns:
        fields.append(T.StructField(c.name, mapping[c.type], nullable=c.nullable))
    return T.StructType(fields)
