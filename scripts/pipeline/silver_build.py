"""Bronze를 읽어 Silver를 만들고 결과를 대조한다.

Bronze 적재와 달리 **전체를 다시 만든다.** 중복 제거가 덤프를 걸쳐 작동하므로
한 덤프만 다시 처리하면 다른 덤프의 중복을 보지 못한다.

    ./scripts/spark_run.sh -- python scripts/pipeline/silver_build.py
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config, job_runs
from flux.common.spark import session as spark_session
from flux.silver import build, schema, user_recording_counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--driver-memory", default="8g")
    args = parser.parse_args()

    bronze_path = config.bronze(bronze_schema.TABLE_NAME)
    silver_path = config.silver(schema.TABLE_NAME)
    quarantine_path = config.silver(schema.QUARANTINE_TABLE_NAME)
    built_at = datetime.now(timezone.utc)

    spark = spark_session("flux-silver-build", shuffle_partitions=args.shuffle_partitions, memory=args.driver_memory)
    started = time.time()
    try:
        silver, rejected = build.build(spark, bronze_path, built_at=built_at)

        build.write(silver, silver_path, partition=schema.PARTITION_COLUMN)
        build.enable_change_feed(spark, silver_path)
        build.write(rejected, quarantine_path,
                    partition=schema.QUARANTINE_PARTITION_COLUMN)
        user_recording_counts.refresh(
            spark, silver_path, config.silver(user_recording_counts.TABLE_NAME))
        elapsed = time.time() - started

        bronze_rows = spark.read.format("delta").load(bronze_path).count()
        silver_rows = spark.read.format("delta").load(silver_path).count()
        quarantine_rows = spark.read.format("delta").load(quarantine_path).count()
        removed = bronze_rows - silver_rows - quarantine_rows

        print(f"생성 시간 : {elapsed:.0f}초")
        print(f"Bronze    : {bronze_rows:,}")
        print(f"Silver    : {silver_rows:,}")
        print(f"격리      : {quarantine_rows:,}")
        print(f"중복 제거 : {removed:,}  ({removed / bronze_rows * 100:.4f}%)")

        silver_df = spark.read.format("delta").load(silver_path)
        sources = {
            r["mbid_source"]: r["count"]
            for r in silver_df.groupBy("mbid_source").count().collect()
        }
        print()
        print("mbid_source 분포")
        for name in ("MAPPING", "CLIENT", "LASTFM", "NONE"):
            n = sources.get(name, 0)
            print(f"  {name:8} {n:>12,}  {n / silver_rows * 100:6.2f}%")

        with_mbid = silver_rows - sources.get("NONE", 0)
        print(f"  MBID 있음 {with_mbid:>10,}  "
              f"{with_mbid / silver_rows * 100:6.2f}%")

        clients = (
            silver_df.groupBy("client_name").count()
                     .orderBy(F.col("count").desc()).limit(8).collect()
        )
        print()
        print("제출 클라이언트 상위 8개")
        for row in clients:
            print(f"  {row['client_name'][:40]:42} {row['count']:>12,}  "
                  f"{row['count'] / silver_rows * 100:5.2f}%")

        unknown = next(
            (r["count"] for r in clients if r["client_name"] == "UNKNOWN"), 0)

        months = silver_df.select(schema.PARTITION_COLUMN).distinct().count()
        print()
        print(f"파티션(달) : {months:,}개")

        payload = {
            "built_at": built_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "bronze_rows": bronze_rows,
            "silver_rows": silver_rows,
            "quarantine_rows": quarantine_rows,
            "duplicates_removed": removed,
            "mbid_source": sources,
            "rows_with_mbid": with_mbid,
            "unknown_client_rows": unknown,
            "partitions": months,
        }
        config.write_measurement("silver-build", payload)

        if removed < 0:
            print()
            print("대조 실패: Silver와 격리의 합이 Bronze보다 많다")
            return 1
        print()
        print("대조 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(job_runs.run(main))
