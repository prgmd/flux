"""덤프를 하나씩 붙인 Silver와 그것을 따라간 팩트가 전체에서 다시 만든 것과 같은지
실데이터로 본다.

운영 표는 읽기만 한다. 임시 자리(scratch)에서 한다.

    1. 마지막 N개 덤프를 뺀 Bronze로 Silver를 만들고 변경 기록을 켠다
    2. 그 Silver에서 팩트를 만든다. 차원은 운영 것을 복사해 쓴다
    3. N개 덤프를 순서대로 하나씩 Silver에 붙이고, 그때마다 팩트가 따라가게 한다
    4. Silver와 팩트를 운영 표와 대조한다

대조는 실행 시각(`_built_at`) 말고 모든 컬럼이다. **번호도 넣는다.** 번호가 줄 내용의
해시라 어느 길로 만들어도 같아야 한다. 예전 번호(monotonically_increasing_id)였다면
이 대조 자체를 할 수 없었다.

운영 표는 31덤프 전체에서 한 번에 만든 것이다. 같으면 "덤프를 하나씩 붙이는 것"과
"전부 모아 한 번에 고르는 것"이 실데이터에서도 같다는 뜻이다
(flux.silver.dedup.pick_arrival).

    ./scripts/spark_run.sh -m 12g -- python scripts/drills/increment_drill.py [--last 7]
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
from flux.gold import fact, fact_listens, scd2
from flux.gold.mart_build import fingerprint
from flux.silver import build, schema

DRILL = "increment"


def summarize(spark, path):
    df = spark.read.format("delta").load(path)
    columns = [c for c in df.columns if c != "_built_at"]
    row = df.agg(F.count(F.lit(1)).alias("rows"), fingerprint(*columns).alias("print")).first()
    return row["rows"], str(row["print"])


def file_count(spark, path) -> int:
    from delta.tables import DeltaTable
    return DeltaTable.forPath(spark, path).detail().first()["numFiles"]


def now():
    return datetime.now(timezone.utc)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--last", type=int, default=7)
    parser.add_argument("--keep", action="store_true",
                        help="끝나도 사본을 지우지 않는다. 조사할 때만 쓴다")
    args = parser.parse_args()

    bronze_path = config.bronze(bronze_schema.TABLE_NAME)
    production = {
        "silver": config.silver(schema.TABLE_NAME),
        "fact": config.gold(fact.TABLE_NAME),
    }
    work_dir = config.local_root() / config.SCRATCH / DRILL
    work = {
        "silver": config.scratch(DRILL, schema.TABLE_NAME),
        "quarantine": config.scratch(DRILL, schema.QUARANTINE_TABLE_NAME),
        "fact": config.scratch(DRILL, fact.TABLE_NAME),
        "dim": config.scratch(DRILL, scd2.TABLE_NAME),
    }

    spark = spark_session("flux-increment-drill", shuffle_partitions=200, memory="8g")
    try:
        dumps = sorted(r[0] for r in spark.read.format("delta").load(bronze_path)
                       .select("_source_dump").distinct().collect())
        held = dumps[-args.last:]
        print(f"덤프 {len(dumps)}개 중 뒤 {len(held)}개를 증분으로 붙인다", flush=True)

        (spark.read.format("delta").load(config.gold(scd2.TABLE_NAME))
         .write.format("delta").mode("overwrite").save(work["dim"]))

        started = time.time()
        base, rejected = build.build(spark, bronze_path, built_at=now(), without=held)
        build.write(base, work["silver"], partition=schema.PARTITION_COLUMN)
        build.write(rejected, work["quarantine"], partition=schema.QUARANTINE_PARTITION_COLUMN)
        build.enable_change_feed(spark, work["silver"])
        silver_seconds = time.time() - started

        started = time.time()
        version = fact_listens.table_version(spark, work["silver"])
        rows = spark.read.format("delta").option("versionAsOf", version).load(work["silver"])
        fact_listens.write(fact_listens.prepare(rows, built_at=now()), work["fact"],
                           silver_version=version)
        fact_seconds = time.time() - started
        base_files = {name: file_count(spark, work[name]) for name in ("silver", "fact")}
        print(f"바탕 Silver {silver_seconds:.0f}초, 팩트 {fact_seconds:.0f}초, 파일 "
              f"Silver {base_files['silver']:,} 팩트 {base_files['fact']:,}", flush=True)

        steps = []
        for dump in held:
            started = time.time()
            changes, rejected = build.increment(spark, bronze_path, work["silver"],
                                                dump=dump, built_at=now())
            silver_merge = build.merge(spark, changes, work["silver"])
            build.replace_quarantine(rejected, work["quarantine"], dump=dump)
            silver_step = time.time() - started

            started = time.time()
            fact_merge = fact_listens.increment(spark, work["silver"], work["fact"],
                                                work["dim"], built_at=now())
            fact_step = time.time() - started

            print(f"{dump[17:31]} Silver {silver_step:4.0f}초 넣음 "
                  f"{silver_merge['numTargetRowsInserted']:>9,} 고침 "
                  f"{silver_merge['numTargetRowsUpdated']:>7,} 파일 "
                  f"{silver_merge['numTargetFilesRemoved']:>5,}→{silver_merge['numTargetFilesAdded']:<4,}"
                  f" | 팩트 {fact_step:4.0f}초 넣음 {fact_merge.get('numTargetRowsInserted', 0):>9,} "
                  f"지움 {fact_merge.get('numTargetRowsDeleted', 0):>7,} 파일 "
                  f"{fact_merge.get('numTargetFilesRemoved', 0):>5,}→"
                  f"{fact_merge.get('numTargetFilesAdded', 0):<4,}", flush=True)
            steps.append({"dump": dump, "silver_seconds": round(silver_step, 1),
                          "silver_merge": silver_merge, "fact_seconds": round(fact_step, 1),
                          "fact_merge": fact_merge})

        results = {}
        for name in ("silver", "fact"):
            expected = summarize(spark, production[name])
            actual = summarize(spark, work[name])
            results[name] = {
                "production": {"rows": expected[0], "fingerprint": expected[1],
                               "files": file_count(spark, production[name])},
                "incremental": {"rows": actual[0], "fingerprint": actual[1],
                                "files": file_count(spark, work[name])},
                "same": expected == actual,
            }
            print(f"{name:6} 운영 {expected[0]:>13,}행 {expected[1]}")
            print(f"{'':6} 증분 {actual[0]:>13,}행 {actual[1]}  "
                  f"{'같다' if expected == actual else '다르다'}", flush=True)
        quarantined = spark.read.format("delta").load(work["quarantine"]).count()
        production_quarantined = spark.read.format("delta").load(
            config.silver(schema.QUARANTINE_TABLE_NAME)).count()
        print(f"격리   운영 {production_quarantined:,}  증분 {quarantined:,}")
        same = (all(r["same"] for r in results.values())
                and quarantined == production_quarantined)
        print("같다" if same else "다르다")

        config.write_measurement("increment-drill", {
            "held_out": held,
            "base_seconds": {"silver": round(silver_seconds, 1), "fact": round(fact_seconds, 1)},
            "base_files": base_files,
            "steps": steps,
            "tables": results,
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
