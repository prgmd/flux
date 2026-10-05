"""주마다 급상승 아티스트 상위를 뽑아 마트로 쓴다.

일별 마트를 날짜 차원의 ISO 주로 묶고, 지난주 대비 비율로 순위를 매긴다. 지난주
청취가 하한(20) 이상인 아티스트만 본다(flux.gold.mart.rising). 일별 마트와 날짜 차원이
먼저 있어야 한다.

    ./scripts/spark_run.sh -m 8g -- python scripts/pipeline/gold_mart_rising.py
"""

import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import dates, mart, mart_build


def main() -> int:
    daily_path = config.gold(mart.DAILY_ARTIST)
    calendar_path = config.gold(dates.DATE_TABLE)
    rising_path = config.gold(mart.RISING_ARTIST)
    built_at = datetime.now(timezone.utc)

    spark = spark_session("flux-gold-mart-rising", shuffle_partitions=200)
    started = time.time()
    try:
        daily = spark.read.format("delta").load(daily_path)
        calendar = spark.read.format("delta").load(calendar_path)
        (mart_build.rising(daily, calendar, built_at=built_at)
         .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .save(rising_path))
        elapsed = time.time() - started

        rising = spark.read.format("delta").load(rising_path)
        rows = rising.count()
        weeks = rising.select("week_start").distinct().count()
        latest = rising.agg(F.max("week_start")).first()[0]
        print(f"소요 : {elapsed:.0f}초")
        print(f"마트 : {rows:,}행, {weeks:,}주")
        print(f"마지막 주 {latest} 상위 5")
        for r in rising.filter(F.col("week_start") == latest).orderBy("rank").limit(5).collect():
            print(f"  {r['rank']:>2} {r['artist_name'] or r['artist_mbid']:<30} "
                  f"{r['previous_listens']:>6,} → {r['listens']:>6,}  {r['growth']:.1f}배")

        config.write_measurement("gold-mart-rising", {
            "built_at": built_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "rows": rows,
            "weeks": weeks,
            "minimum_previous_listens": mart.MIN_PREVIOUS_LISTENS,
            "top": mart.RISING_TOP,
        })
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
