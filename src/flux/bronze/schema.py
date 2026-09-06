"""Bronze 계층 스키마 정의.

pyspark를 최상단에서 import하지 않는다. 스키마를 선언과 변환으로 나눠,
선언 부분은 Spark 없이도 읽고 검증할 수 있게 한다. Spark는 컨테이너 안에만
설치되어 있으므로 호스트에서 도는 테스트가 여기에 묶이면 안 된다.
"""

from dataclasses import dataclass

TABLE_NAME = "raw_listens"
QUARANTINE_TABLE_NAME = "invalid_listens"

# 파티션 기준. 적재 시각 기준이라 재실행 단위와 일치한다.
PARTITION_COLUMN = "ingested_date"


@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool
    doc: str


# 명세 5.1을 따르되 Kafka 관련 두 컬럼은 대체했다.
# MVP에서 Kafka가 빠졌으므로 `_kafka_partition`과 `_kafka_offset`은 채울 값이 없다.
# 같은 목적(원본 위치 추적)을 파일 이름과 행 식별자로 달성한다.
COLUMNS: tuple[Column, ...] = (
    Column("_ingested_at", "timestamp", False, "적재 시각"),
    Column("_source_dump", "string", False, "출처 덤프 식별자. 매니페스트 키와 같다"),
    Column("_source_file", "string", False, "덤프 안의 파일 이름"),
    Column("_record_id", "long", False, "적재 안에서 유일한 식별자. 물리적 줄 번호가 아니다"),
    Column("listened_at", "long", True, "청취 시각. 파티션이 아니라 조회 편의용으로만 뽑는다"),
    Column("user_name", "string", True, "사용자 이름. 파생용으로만 뽑는다"),
    Column("raw_json", "string", False, "원본 문서 전체를 문자열 그대로"),
    Column(PARTITION_COLUMN, "date", False, "적재 날짜. 파티션 컬럼"),
)

QUARANTINE_COLUMNS: tuple[Column, ...] = (
    Column("_ingested_at", "timestamp", False, "적재 시각"),
    Column("_source_dump", "string", False, "출처 덤프 식별자"),
    Column("_source_file", "string", False, "덤프 안의 파일 이름"),
    Column("_record_id", "long", False, "적재 안에서 유일한 식별자"),
    Column("raw_line", "string", False, "해석하지 못한 원본 줄"),
    Column("error", "string", False, "실패 사유 한 줄"),
    Column(PARTITION_COLUMN, "date", False, "적재 날짜. 파티션 컬럼"),
)

# 원본에서 뽑아 컬럼으로 올리는 값들. 나머지는 raw_json에만 남는다.
DERIVED_FIELDS = ("listened_at", "user_name")


def column_names(columns: tuple[Column, ...] = COLUMNS) -> list[str]:
    return [c.name for c in columns]


def ddl(columns: tuple[Column, ...] = COLUMNS) -> str:
    """Spark가 읽는 DDL 문자열. 타입 선언을 한곳에서만 관리하기 위한 것."""
    parts = []
    for c in columns:
        suffix = "" if c.nullable else " NOT NULL"
        parts.append(f"{c.name} {c.type}{suffix}")
    return ", ".join(parts)


SPARK_TYPES = ("string", "long", "int", "double", "boolean", "timestamp", "date")


def to_spark_schema(columns: tuple[Column, ...] = COLUMNS):
    """Spark 스키마 객체로 바꾼다. 이 함수만 pyspark를 필요로 한다.

    DDL 문자열을 파싱하는 방법도 있지만 그쪽은 활성 SparkSession을 요구한다.
    스키마를 만드는 데 세션이 필요하면 잡을 짜기 전에 세션부터 띄워야 한다.
    타입을 직접 매핑하면 import만으로 끝난다.
    """
    from pyspark.sql import types as T

    mapping = {
        "string": T.StringType(),
        "long": T.LongType(),
        "int": T.IntegerType(),
        "double": T.DoubleType(),
        "boolean": T.BooleanType(),
        "timestamp": T.TimestampType(),
        "date": T.DateType(),
    }
    fields = []
    for c in columns:
        if c.type not in mapping:
            raise ValueError(f"모르는 타입: {c.type} ({c.name})")
        fields.append(T.StructField(c.name, mapping[c.type], nullable=c.nullable))
    return T.StructType(fields)
