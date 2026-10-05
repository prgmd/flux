"""silver.mb_artist를 gold.dim_artist로 병합하고 불변식을 확인한다.

처음이면 통째로 만들고, 있으면 MERGE 한 문장으로 이력을 쌓는다.

    ./scripts/spark_run.sh -- python scripts/pipeline/gold_dim_artist.py
"""

import argparse
import sys
import time
from datetime import date

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from flux.common import config, job_runs
from flux.common.spark import session as spark_session
from flux.gold import dim_artist, scd2

SILVER_TABLE = "mb_artist"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--at", default=None, help="이력을 끊을 날짜. 없으면 오늘")
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--no-broadcast", action="store_true",
                        help="차원 조인에 브로드캐스트를 강제하지 않는다. AQE가 고르게 두고 재려고 쓴다")
    args = parser.parse_args()

    at = args.at or date.today().isoformat()
    silver_path = config.silver(SILVER_TABLE)
    gold_path = config.gold(scd2.TABLE_NAME)

    spark = spark_session("flux-gold-dim-artist",
                          shuffle_partitions=args.shuffle_partitions,
                          memory="6g")
    started = time.time()
    try:
        silver = spark.read.format("delta").load(silver_path)
        release = silver.select("_release").first()["_release"]
        prepared = dim_artist.prepare(silver, at=at, release=release)

        source_rows = silver.count()
        exists = DeltaTable.isDeltaTable(spark, gold_path)

        print(f"기준일   : {at}")
        print(f"릴리스   : {release}")
        print(f"원본     : {source_rows:,}행")
        print(f"차원     : {'있음, 병합한다' if exists else '없음, 새로 만든다'}")
        print()

        if exists:
            before = spark.read.format("delta").load(gold_path).count()
            dim_artist.merge(spark, gold_path, prepared, at=at,
                             broadcast=not args.no_broadcast)
            after = spark.read.format("delta").load(gold_path).count()
            print(f"병합 전  : {before:,}행")
            print(f"병합 후  : {after:,}행")
            print(f"늘어난 줄: {after - before:,}")
        else:
            dim_artist.create(prepared, gold_path)
            after = spark.read.format("delta").load(gold_path).count()
            before = 0
            print(f"초기 적재: {after:,}행")

        elapsed = time.time() - started
        dim = spark.read.format("delta").load(gold_path)
        current = dim.filter(F.col(scd2.IS_CURRENT)).count()
        keys = dim.select(scd2.BUSINESS_KEY).distinct().count()
        versions = dim.count()

        print()
        print(f"소요     : {elapsed:.0f}초")
        print(f"아티스트 : {keys:,}")
        print(f"이력 줄  : {versions:,}  (현재 {current:,})")
        print(f"평균 버전: {versions / keys:.4f}")

        checks = dim_artist.invariants(spark, gold_path)
        print()
        print("SCD2 불변식")
        labels = {
            "keys_without_exactly_one_current": "현재 줄이 정확히 1개가 아닌 아티스트",
            "keys_with_no_current": "현재 줄이 없는 아티스트",
            "rows_with_invalid_range": "유효 구간이 뒤집힌 줄",
        }
        for name, value in checks.items():
            mark = "통과" if value == 0 else f"위반 {value:,}건"
            print(f"  {labels[name]:34} {mark}")

        config.write_measurement("dim-artist-merge", {
            "at": at,
            "release": release,
            "elapsed_seconds": round(elapsed, 1),
            "source_rows": source_rows,
            "rows_before": before,
            "rows_after": versions,
            "distinct_keys": keys,
            "current_rows": current,
            "invariants": checks,
        })

        if any(checks.values()):
            print()
            print("불변식 위반이 있다")
            return 1
        print()
        print("불변식 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(job_runs.run(main))
