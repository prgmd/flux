"""날짜 차원과 사용자 차원의 선언과 판단. pyspark를 부르지 않는다.

Spark 표현은 flux.gold.dim_date와 flux.gold.dim_user에 있다. 여기 있는 것은 그 둘이
따라야 하는 규칙과, 그 규칙을 Spark 없이 확인할 수 있는 구현이다.

**날짜 차원**은 기간의 하루하루를 한 줄씩 미리 만들어 둔 표다. 청취가 없는 날도 줄이
있다. 그래야 "청취가 0인 날"이 집계에서 빠지지 않고 0으로 나온다. 주 단위 집계(급상승
마트)는 이 표의 ISO 주를 쓴다. 해의 첫 주가 어디서 시작하는지를 잡마다 따로 계산하면
갈라진다.

**사용자 차원**은 사용자 번호마다 한 줄이다. 이름은 가장 최근 청취에 쓰인 이름으로
덮어쓴다(Type 1). 이름을 바꾼 번호가 17개(0.04%)뿐이고, 이름 변경은 정정 성격이라
이력을 남기지 않는다(docs/measurements/2026-09-14-user-identity.md).
"""

from datetime import date, timedelta

from flux.silver.schema import Column

DATE_TABLE = "dim_date"
USER_TABLE = "dim_user"

DATE_COLUMNS: tuple[Column, ...] = (
    Column("date", "date", False, "날짜. 키"),
    Column("year", "int", False, "연"),
    Column("month", "int", False, "월"),
    Column("day", "int", False, "일"),
    Column("listened_month", "string", False, "연월(yyyy-MM). 팩트의 파티션 값과 같다"),
    Column("iso_year", "int", False, "ISO 주가 속한 해. 1월 초는 앞해일 수 있다"),
    Column("iso_week", "int", False, "ISO 주 번호(1~53)"),
    Column("day_of_week", "int", False, "요일. 월요일 1, 일요일 7"),
    Column("is_weekend", "boolean", False, "토요일이나 일요일"),
    Column("week_start", "date", False, "그 주의 월요일"),
)

USER_COLUMNS: tuple[Column, ...] = (
    Column("user_id", "string", False, "사용자 번호. 키"),
    Column("user_name", "string", False, "가장 최근 청취에 쓰인 이름"),
    Column("first_listened_at", "timestamp", False, "첫 청취"),
    Column("last_listened_at", "timestamp", False, "마지막 청취"),
    Column("listens", "long", False, "Silver 청취 수"),
    Column("_built_at", "timestamp", False, "생성 시각"),
)


def column_names(columns) -> list[str]:
    return [c.name for c in columns]


def attributes(day: date) -> dict:
    """하루의 날짜 차원 한 줄.

    ISO 주는 월요일에 시작하고, 그해의 첫 목요일이 든 주가 1주다. 그래서 1월 1일이
    금요일이면 그날은 앞해의 53주다. 연(`year`)과 ISO 주의 해(`iso_year`)를 따로 둔다.
    """
    iso_year, iso_week, iso_day = day.isocalendar()
    return {
        "date": day,
        "year": day.year,
        "month": day.month,
        "day": day.day,
        "listened_month": day.strftime("%Y-%m"),
        "iso_year": iso_year,
        "iso_week": iso_week,
        "day_of_week": iso_day,
        "is_weekend": iso_day >= 6,
        "week_start": day - timedelta(days=iso_day - 1),
    }


def span(first: date, last: date) -> list:
    """양끝을 포함한 기간의 날짜들. 날짜 차원의 줄 수가 이 길이와 같아야 한다."""
    if last < first:
        raise ValueError(f"기간이 거꾸로다: {first} ~ {last}")
    return [first + timedelta(days=n) for n in range((last - first).days + 1)]


def current_name(observations) -> str:
    """(청취 시각, 이름) 기록에서 사용자 차원에 둘 이름.

    가장 최근 청취의 이름이다. 같은 시각에 이름이 둘이면 사전순으로 뒤의 것을 고른다.
    무엇을 고르든 상관없지만 실행마다 같아야 한다.
    """
    rows = list(observations)
    if not rows:
        raise ValueError("이름을 고를 기록이 없다")
    return max(rows)[1]
