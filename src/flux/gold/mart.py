"""집계 마트의 선언. pyspark를 부르지 않는다.

이름과 상수만 둔다. 실제 집계는 flux.gold.mart_build가 한다.

**나누는 이유가 있다.** 정합성 오라클은 Spark가 아닌 엔진으로 돌아야 하는데,
마트 이름 하나를 얻으려고 pyspark를 import하면 그 순간 Spark 없이는 못 돈다.
오라클이 검증 대상과 같은 의존을 지는 셈이라 독립성이 깨진다.

flux.gold.fact와 flux.gold.fact_listens, flux.gold.scd2와 flux.gold.dim_artist가
같은 이유로 나뉘어 있다.
"""

DAILY_ARTIST = "mart_daily_artist"
