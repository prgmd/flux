"""증분으로 붙인 Silver가 전체에서 다시 만든 Silver와 같은지 실데이터로 본다.

운영 표는 읽기만 한다. 임시 자리(scratch)에서 한다.

    1. 마지막 N개 덤프를 뺀 Bronze로 Silver를 만든다
    2. 그 N개를 덤프 순서대로 하나씩 증분으로 붙인다. 덤프마다 시간과 병합 지표를 남긴다
    3. 결과를 운영 Silver와 대조한다

대조는 실행 시각(`_built_at`) 말고 모든 컬럼이다. **번호도 넣는다.** 번호가 줄 내용의
해시라 어느 길로 만들어도 같아야 한다. 예전 번호(monotonically_increasing_id)였다면
이 대조 자체를 할 수 없었다.

운영 Silver는 31덤프 전체에서 한 번에 만든 것이다. 같으면 "덤프를 하나씩 붙이는 것"과
"전부 모아 한 번에 고르는 것"이 실데이터에서도 같다는 뜻이다(flux.silver.dedup.pick_arrival).

    ./scripts/spark_run.sh -m 12g -- python scripts/drills/silver_increment_drill.py [--last 7]
"""

import argparse
import shutil
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold.mart_build import fingerprint
from flux.silver import build, schema

DRILL = "silver-increment"


def summarize(spark, path):
    df = spark.read.format("delta").load(path)
    columns = [c for c in df.columns if c != "_built_at"]
    row = df.agg(F.count(F.lit(1)).alias("rows"), fingerprint(*columns).alias("print")).first()
    return row["rows"], str(row["print"])


def file_count(spark, path) -> int:
    from delta.tables import DeltaTable
    return DeltaTable.forPath(spark, path).detail().first()["numFiles"]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--last", type=int, default=7)
    parser.add_argument("--keep", action="store_true",
                        help="끝나도 사본을 지우지 않는다. 조사할 때만 쓴다")
    args = parser.parse_args()

    bronze_path = config.bronze(bronze_schema.TABLE_NAME)
    production = config.silver(schema.TABLE_NAME)
    work_dir = config.local_root() / config.SCRATCH / DRILL
    work_silver = config.scratch(DRILL, schema.TABLE_NAME)
    work_quarantine = config.scratch(DRILL, schema.QUARANTINE_TABLE_NAME)

    spark = spark_session("flux-silver-increment-drill", shuffle_partitions=200, memory="8g")
    try:
        dumps = sorted(r[0] for r in spark.read.format("delta").load(bronze_path)
                       .select("_source_dump").distinct().collect())
        held = dumps[-args.last:]
        print(f"덤프 {len(dumps)}개 중 뒤 {len(held)}개를 증분으로 붙인다")

        started = time.time()
        base, rejected = build.build(spark, bronze_path, built_at=datetime.now(timezone.utc),
                                     without=held)
        build.write(base, work_silver, partition=schema.PARTITION_COLUMN)
        build.write(rejected, work_quarantine, partition=schema.QUARANTINE_PARTITION_COLUMN)
        base_seconds = time.time() - started
        base_files = file_count(spark, work_silver)
        print(f"바탕 Silver {base_seconds:.0f}초, 파일 {base_files:,}개", flush=True)

        steps = []
        for dump in held:
            started = time.time()
            changes, rejected = build.increment(spark, bronze_path, work_silver, dump=dump,
                                                built_at=datetime.now(timezone.utc))
            metrics = build.merge(spark, changes, work_silver)
            build.replace_quarantine(rejected, work_quarantine, dump=dump)
            seconds = time.time() - started
            print(f"{dump[17:31]} {seconds:5.0f}초  넣음 {metrics['numTargetRowsInserted']:>9,}  "
                  f"고침 {metrics['numTargetRowsUpdated']:>7,}  "
                  f"파일 지움 {metrics['numTargetFilesRemoved']:>5,} 새로 씀 "
                  f"{metrics['numTargetFilesAdded']:>4,}", flush=True)
            steps.append({"dump": dump, "seconds": round(seconds, 1), "merge": metrics})

        expected = summarize(spark, production)
        actual = summarize(spark, work_silver)
        quarantined = spark.read.format("delta").load(work_quarantine).count()
        production_quarantined = spark.read.format("delta").load(
            config.silver(schema.QUARANTINE_TABLE_NAME)).count()
        same = expected == actual and quarantined == production_quarantined

        print()
        print(f"운영 Silver  {expected[0]:>13,}행  지문 {expected[1]}")
        print(f"증분 Silver  {actual[0]:>13,}행  지문 {actual[1]}")
        print(f"격리         운영 {production_quarantined:,}  증분 {quarantined:,}")
        print(f"파일         운영 {file_count(spark, production):,}  "
              f"증분 바탕 {base_files:,} → 끝 {file_count(spark, work_silver):,}")
        print("같다" if same else "다르다")

        config.write_measurement("silver-increment-drill", {
            "held_out": held,
            "base_seconds": round(base_seconds, 1),
            "base_files": base_files,
            "steps": steps,
            "production": {"rows": expected[0], "fingerprint": expected[1]},
            "incremental": {"rows": actual[0], "fingerprint": actual[1]},
            "quarantine": {"production": production_quarantined, "incremental": quarantined},
            "same": same,
        })
        return 0 if same else 1
    finally:
        spark.stop()
        if not args.keep and work_dir.exists():
            shutil.rmtree(work_dir)


if __name__ == "__main__":
    sys.exit(main())
