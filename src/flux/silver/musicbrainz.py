"""MusicBrainz 덤프 테이블의 컬럼 순서와 값 해석 규칙.

덤프 파일에는 헤더가 없다. PostgreSQL COPY 형식이라 컬럼 이름이 파일 어디에도
없고, 순서는 MusicBrainz 스키마 정의에서 와야 한다.

**순서를 틀리면 프로그램은 그대로 돌고 결과만 조용히 틀린다.** 이름 자리에
나라 번호가 들어가도 문자열이므로 아무 데서도 걸리지 않는다. 그래서 선언을
한곳에 모으고, 읽는 쪽에서 컬럼 개수를 확인한다.

pyspark를 부르지 않는다. 컬럼 순서와 값 해석은 Spark가 필요 없는 판단이다.

날짜 조립과 boolean 해석은 Spark 잡이 자기 식으로 하고 있어 여기서 뺐다.
두 벌로 두면 갈라지고, 안 쓰이는 쪽은 갈라진 줄도 모른다.
"""

NULL = "\\N"

# 스키마 정의에서 온 컬럼 순서. 값의 모양으로 검산했다.
# 근거는 docs/measurements/2026-09-15-mb-artist-shape.md.
TABLE_COLUMNS = {
    "artist": (
        "id", "gid", "name", "sort_name",
        "begin_date_year", "begin_date_month", "begin_date_day",
        "end_date_year", "end_date_month", "end_date_day",
        "type", "area", "gender", "comment",
        "edits_pending", "last_updated", "ended",
        "begin_area", "end_area",
    ),
    "area": (
        "id", "gid", "name", "type", "edits_pending", "last_updated",
        "begin_date_year", "begin_date_month", "begin_date_day",
        "end_date_year", "end_date_month", "end_date_day", "ended", "comment",
    ),
    "artist_type": ("id", "name", "parent", "child_order", "description", "gid"),
    "gender": ("id", "name", "parent", "child_order", "description", "gid"),
    "iso_3166_1": ("area", "code"),
}

# dim_artist를 만들려면 이 다섯이 모두 필요하다. artist의 type, area, gender가
# 숫자 외래 키이고, area는 국가가 아니라 iso_3166_1을 거쳐야 하기 때문이다.
REQUIRED_TABLES = tuple(TABLE_COLUMNS)


class ColumnCountError(ValueError):
    """행의 컬럼 개수가 선언과 다를 때 발생한다."""


def columns(table: str) -> tuple[str, ...]:
    if table not in TABLE_COLUMNS:
        raise KeyError(f"모르는 테이블: {table}")
    return TABLE_COLUMNS[table]


def unnull(value):
    """덤프의 NULL 표기를 None으로. 빈 문자열은 값이므로 그대로 둔다.

    `comment`가 빈 문자열로 들어온다. 없는 것과 빈 것은 다르다.
    """
    return None if value == NULL else value


def parse_row(table: str, line: str) -> dict:
    """한 줄을 컬럼 이름이 붙은 사전으로. 개수가 다르면 거부한다.

    조용히 넘어가면 컬럼이 한 칸씩 밀린 채로 적재된다. 그 상태는
    타입 검사에도 안 걸리므로 여기서 막는 수밖에 없다.
    """
    names = columns(table)
    fields = line.rstrip("\n").split("\t")
    if len(fields) != len(names):
        raise ColumnCountError(
            f"{table}: 컬럼 {len(fields)}개, 선언은 {len(names)}개")
    return {name: unnull(value) for name, value in zip(names, fields)}
