"""청취 시각의 분포를 재서 파티션 키를 정할 근거를 만든다.

명세는 Silver와 Gold를 `listened_date`로 파티션하라고 한다. 그런데 하루치 덤프
안에 20년 전 기록이 섞여 들어온다는 것을 이미 확인했다. 그대로 나누면 디렉터리가
날짜 수만큼 생기고, 그중 대부분이 몇 행짜리 조각이 된다.

파티션은 읽을 때 안 읽어도 되는 것을 건너뛰려고 나눈다. 너무 잘게 나누면
목록을 관리하는 비용이 읽기에서 아끼는 비용을 넘어선다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/analyze_listened_at.py
"""

import argparse
import sys

from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.common.spark import session as spark_session


# 파티션 하나가 이보다 작으면 나눈 값어치가 없다고 본다.
# Parquet 권장 파일 크기가 128MB 안팎이고, 한 행이 대략 170바이트이므로
# 그 정도를 채우려면 수십만 행이 필요하다. 여기서는 판단 기준을 느슨하게 잡는다.
TINY_ROWS = 100_000


def main() -> int:
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    root = config.local_root()
    spark = spark_session("flux-listened-at", memory="4g")
    try:
        df = (
            spark.read.format("delta").load(config.bronze(bronze_schema.TABLE_NAME))
            .select("listened_at")
            .withColumn("ts", F.col("listened_at").cast("timestamp"))
            .withColumn("day", F.to_date("ts"))
            .withColumn("year", F.year("ts"))
            .cache()
        )

        total = df.count()
        null_ts = df.filter(F.col("day").isNull()).count()
        print(f"전체 행      : {total:,}")
        print(f"시각 없음    : {null_ts:,}")

        valid = df.filter(F.col("day").isNotNull())

        by_day = valid.groupBy("day").count().cache()
        days = by_day.count()
        tiny = by_day.filter(F.col("count") < TINY_ROWS).count()
        tiny_rows = by_day.filter(F.col("count") < TINY_ROWS) \
                          .agg(F.sum("count")).collect()[0][0] or 0

        print()
        print(f"서로 다른 날 : {days:,}")
        print(f"작은 날      : {tiny:,}개 ({tiny / days * 100:.1f}%), "
              f"합쳐서 {tiny_rows:,}행 ({tiny_rows / total * 100:.2f}%)")

        stats = by_day.agg(
            F.min("count").alias("최소"),
            F.expr("percentile_approx(count, 0.5)").alias("중앙값"),
            F.max("count").alias("최대"),
        ).collect()[0]
        print(f"하루당 행    : 최소 {stats['최소']:,}, "
              f"중앙값 {stats['중앙값']:,}, 최대 {stats['최대']:,}")

        by_year = valid.groupBy("year").count().orderBy("year").collect()
        print()
        print("연도별 (상위 12개)")
        top = sorted(by_year, key=lambda r: -r["count"])[:12]
        for row in top:
            share = row["count"] / total * 100
            bar = "#" * max(1, round(share / 2))
            print(f"  {row['year']}  {row['count']:>12,}  {share:5.1f}%  {bar}")

        recent = sum(r["count"] for r in by_year if r["year"] and r["year"] >= 2025)
        print()
        print(f"2025년 이후  : {recent:,} ({recent / total * 100:.1f}%)")
        print(f"연도 수      : {len(by_year)}")

        by_month = (
            valid.groupBy(F.date_format("day", "yyyy-MM").alias("month"))
                 .count().cache()
        )
        months = by_month.count()
        month_stats = by_month.agg(
            F.min("count").alias("최소"),
            F.expr("percentile_approx(count, 0.5)").alias("중앙값"),
            F.max("count").alias("최대"),
        ).collect()[0]
        tiny_months = by_month.filter(F.col("count") < TINY_ROWS).count()
        top_months = by_month.orderBy(F.col("count").desc()).limit(6).collect()

        print()
        print(f"서로 다른 달 : {months:,}")
        print(f"한 달당 행   : 최소 {month_stats['최소']:,}, "
              f"중앙값 {month_stats['중앙값']:,}, 최대 {month_stats['최대']:,}")
        print(f"작은 달      : {tiny_months:,}개 ({tiny_months / months * 100:.1f}%)")
        print("큰 달 6개    : " + ", ".join(
            f"{r['month']} {r['count']:,}" for r in top_months))

        row_bytes = 171
        print()
        print("파티션 키 후보 (한 조각의 크기, 행당 171바이트 가정)")
        for label, n, median, biggest in (
            ("listened_date", days, stats["중앙값"], stats["최대"]),
            ("listened_month", months, month_stats["중앙값"], month_stats["최대"]),
            ("listened_year", len(by_year), None, max(r["count"] for r in by_year)),
        ):
            mid = f"{median * row_bytes / 1024 ** 2:8.1f}MB" if median else "        -"
            top = biggest * row_bytes / 1024 ** 2
            print(f"  {label:16} 조각 {n:>6,}개   중앙값 {mid}   최대 {top:9.1f}MB")

        payload = {
            "total_rows": total,
            "null_timestamps": null_ts,
            "distinct_days": days,
            "distinct_months": months,
            "distinct_years": len(by_year),
            "tiny_days": tiny,
            "tiny_day_rows": tiny_rows,
            "tiny_months": tiny_months,
            "rows_per_month_min": month_stats["최소"],
            "rows_per_month_median": month_stats["중앙값"],
            "rows_per_month_max": month_stats["최대"],
            "tiny_threshold": TINY_ROWS,
            "rows_per_day_min": stats["최소"],
            "rows_per_day_median": stats["중앙값"],
            "rows_per_day_max": stats["최대"],
            "by_year": {str(r["year"]): r["count"] for r in by_year},
            "top_months": {r["month"]: r["count"] for r in top_months},
        }
        config.write_measurement("listened-at-distribution", payload)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
