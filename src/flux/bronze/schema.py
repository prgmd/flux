"""Bronze 계층 스키마 정의.

컬럼을 한곳에 선언해 두고 적재 잡이 이름 목록만 가져다 쓴다.
pyspark를 부르지 않으므로 Spark 없이도 읽고 검증할 수 있다.
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


def column_names(columns: tuple[Column, ...] = COLUMNS) -> list[str]:
    return [c.name for c in columns]
