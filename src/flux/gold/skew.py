"""데이터 스큐를 다루는 규칙. 명세 6.4의 실험이 여기 기댄다.

**스큐란 한 파티션에 일이 몰리는 것이다.** Spark는 같은 키를 같은 파티션으로
보내므로, 키의 분포가 고르지 않으면 파티션의 크기도 고르지 않다.

전체 소요 시간은 **가장 느린 태스크**가 정한다. 나머지 199개가 1초에 끝나도
하나가 5분 걸리면 그 단계는 5분이다. 코어를 늘려도 안 줄어든다.

청취 데이터에서 이것은 인위적으로 만든 것이 아니다. 인기 아티스트에 청취가
몰리는 것이 **실제 분포**다.

## 소금을 치는 이유

한 키가 너무 크면 그 키를 **쪼갠다.** 키 뒤에 임의의 번호를 붙이면 하나였던
키가 여러 개가 되고, 여러 파티션에 나뉜다.

    artist_X            →  artist_X#0, artist_X#1, ... artist_X#15

나눠서 센 다음 소금을 떼고 다시 합친다. 두 번 세는 대신 한 태스크가 짊어지던
일을 열여섯으로 나눈다.

**집계는 이것이 되고 조인은 조건이 붙는다.** 집계는 부분합을 더하면 되지만,
조인은 반대쪽도 같은 소금을 갖고 있어야 짝이 맞는다. 그래서 반대쪽을 소금
가짓수만큼 복제한다. 작은 쪽이어야 복제가 감당된다.

pyspark를 부르지 않는다. 실제 실행은 flux.gold.mart가 한다.
"""

from dataclasses import dataclass

# 소금 가짓수. 한 키를 몇 조각으로 나눌 것인가.
#
# 크면 잘 쪼개지지만 조인에서 반대쪽을 그만큼 복제해야 하고, 집계에서는
# 2단계의 입력이 그만큼 늘어난다. 작으면 쪼개는 효과가 모자란다.
# 16은 출발점이고, 실험에서 이 값 자체를 바꿔 보지는 않는다.
SALT_BUCKETS = 16

# 키와 소금을 잇는 글자. MBID는 UUID라 이 글자를 포함하지 않으므로
# 떼어낼 때 모호함이 없다.
SALT_SEPARATOR = "#"


def salted(key: str, salt: int) -> str:
    """키에 소금을 친다. 하나였던 키가 SALT_BUCKETS개로 갈라진다."""
    if salt < 0 or salt >= SALT_BUCKETS:
        raise ValueError(f"소금은 0 이상 {SALT_BUCKETS} 미만이다: {salt}")
    return f"{key}{SALT_SEPARATOR}{salt}"


def unsalted(key: str) -> str:
    """소금을 뗀다. 2단계 집계가 원래 키로 다시 묶을 때 쓴다.

    마지막 구분자에서 자른다. 키 자체에 구분자가 있어도 견디게 하려는 것인데,
    MBID는 UUID라 실제로는 그런 경우가 없다.
    """
    head, sep, _ = key.rpartition(SALT_SEPARATOR)
    return head if sep else key


@dataclass(frozen=True)
class Config:
    """실험 한 구성. 명세 6.4의 4단계 비교표가 이 넷이다."""

    name: str
    aqe: bool
    salting: bool
    label: str

    def spark_conf(self) -> dict:
        """이 구성이 요구하는 Spark 설정.

        AQE를 끌 때 skewJoin도 함께 끈다. skewJoin은 AQE의 기능이라
        AQE가 꺼져 있으면 무의미하지만, 명시해 두어야 설정을 읽는 사람이
        "이건 어떻게 되어 있지"를 되묻지 않는다.
        """
        return {
            "spark.sql.adaptive.enabled": str(self.aqe).lower(),
            "spark.sql.adaptive.skewJoin.enabled": str(self.aqe).lower(),
        }


# 명세 6.4의 표 그대로다. 순서가 곧 읽는 순서다.
#
# **1번이 기준선이다.** Spark 3부터 AQE가 기본 켜짐이라, 끄지 않으면
# "무처리 기준선"이 이미 최적화된 상태가 되고 salting의 효과가 과소평가된다.
CONFIGS = (
    Config("aqe_off", aqe=False, salting=False, label="AQE 비활성 (기준선)"),
    Config("aqe_on", aqe=True, salting=False, label="AQE 활성"),
    Config("salt_only", aqe=False, salting=True, label="AQE 비활성 + 수동 Salting"),
    Config("both", aqe=True, salting=True, label="AQE 활성 + Salting"),
)

# 구성마다 몇 번 돌릴 것인가. 한 번만 재면 그날의 잡음과 효과가 구분되지 않는다.
REPEATS = 3


def config(name: str) -> Config:
    for c in CONFIGS:
        if c.name == name:
            return c
    raise KeyError(f"모르는 구성: {name}")


def skewness(counts) -> dict:
    """치우침이 실제로 있는지 센다. **기법을 적용하기 전에 이것부터 본다.**

    없는 병목에 salting을 걸면 소금값만 치르고 얻는 것이 없다. 명세가
    "기법을 적용하기 전에 병목이 실재하는지 확인하는 순서"라고 적은 이유다.

    돌려주는 것 중 `max_over_median`이 핵심이다. 가장 큰 파티션이 중앙값의
    몇 배인가. 1에 가까우면 고르고, 크면 한 태스크가 그만큼 오래 붙잡는다.
    """
    rows = sorted(counts)
    if not rows:
        raise ValueError("셀 것이 없다")
    total = sum(rows)
    middle = len(rows) // 2
    median = (rows[middle] if len(rows) % 2
              else (rows[middle - 1] + rows[middle]) / 2)
    return {
        "keys": len(rows),
        "total": total,
        "max": rows[-1],
        "median": median,
        "max_over_median": rows[-1] / median if median else float("inf"),
        "top1_share": rows[-1] / total if total else 0.0,
    }
