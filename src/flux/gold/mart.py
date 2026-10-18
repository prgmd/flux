"""집계 마트의 선언. pyspark를 부르지 않는다.

이름과 상수만 둔다. 실제 집계는 flux.gold.mart_build가 한다.

**나누는 이유가 있다.** 정합성 오라클은 Spark가 아닌 엔진으로 돌아야 하는데,
마트 이름 하나를 얻으려고 pyspark를 import하면 그 순간 Spark 없이는 못 돈다.
오라클이 검증 대상과 같은 의존을 지는 셈이라 독립성이 깨진다.

flux.gold.fact와 flux.gold.fact_listens, flux.gold.scd2와 flux.gold.dim_artist가
같은 이유로 나뉘어 있다.
"""

DAILY_ARTIST = "mart_daily_artist"

PERIOD_ARTIST = "mart_period_artist"

RISING_ARTIST = "mart_rising_artist"

RISING_TOP = 20

MIN_PREVIOUS_LISTENS = 20


def rising(this_week: dict, previous_week: dict, *, top: int = RISING_TOP,
           minimum: int = MIN_PREVIOUS_LISTENS) -> list:
    """한 주의 급상승 아티스트. (아티스트, 이번 주, 지난주)를 순위대로 돌려준다.

    순위는 지난주 대비 비율이다. 비율이 같으면 이번 주 청취가 많은 쪽, 그것도 같으면
    아티스트 MBID 순이다. 마지막 기준이 없으면 같은 입력에 다른 순위가 나온다.

    **지난주 청취가 `minimum` 이상인 아티스트만 본다.** 비율은 작은 수에서 요동친다.
    지난주 1번, 이번 주 30번이면 30배다. 하한은 실측으로 정했다
    (scripts/investigations/measure_rising_threshold.py). 하한이 없으면 덤프 기간 4주의
    상위 80자리 중 21자리가 청취자 한 명이 만든 급상승이었고, 20에서 1자리가 됐다. 그
    위로 올려도 1~2자리에서 줄지 않고 남는 아티스트만 절반씩 줄었다.

    지난주에 없던 아티스트는 비율을 낼 수 없어 빠진다. 이번 주에 없는 아티스트는
    줄었으니 대상이 아니다.
    """
    candidates = [(artist, listens, previous_week[artist])
                  for artist, listens in this_week.items()
                  if previous_week.get(artist, 0) >= minimum]
    candidates.sort(key=lambda c: (-c[1] / c[2], -c[1], c[0]))
    return candidates[:top]
