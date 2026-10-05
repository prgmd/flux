"""silver.listens_normalized를 gold.fact_listens로 싣는다.

순서가 중요하다. **자리를 먼저 만들고 팩트를 쓴다.**

    1. 펼친다
    2. 차원에 없는 아티스트를 찾아 placeholder를 차원에 넣는다
    3. 팩트를 쓴다. 읽은 Silver 버전을 처리 위치로 같은 커밋에 적는다
    4. 불변식을 확인한다

2번을 3번 뒤로 미루면, 불변식을 확인하는 순간에 고아 아티스트가 있는 상태가
된다. 잡이 스스로 만든 위반을 스스로 고치는 모양이 되어 검증이 무의미해진다.

    ./scripts/spark_run.sh -- python scripts/pipeline/gold_fact_listens.py
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from pyspark.sql import functions as F

from flux.common import config, job_runs
from flux.common.spark import session as spark_session
from flux.gold import fact, fact_listens, scd2
from flux.silver import schema as silver_schema


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--skip-placeholders", action="store_true",
                        help="자리를 만들지 않는다. 불변식이 깨지는지 보려고 쓴다")
    args = parser.parse_args()

    silver_path = config.silver(silver_schema.TABLE_NAME)
    dim_path = config.gold(scd2.TABLE_NAME)
    fact_path = config.gold(fact.TABLE_NAME)

    spark = spark_session("flux-gold-fact-listens",
                          shuffle_partitions=args.shuffle_partitions)
    started = time.time()
    try:
        built_at = datetime.now(timezone.utc)
        silver_version = fact_listens.table_version(spark, silver_path)
        silver = (spark.read.format("delta")
                  .option("versionAsOf", silver_version).load(silver_path))
        dim = spark.read.format("delta").load(dim_path)

        # 행 수와 배열 안 중복을 한 번에 센다. count()를 따로 부르면
        # 1.5억 행을 두 번 훑는다.
        counted = silver.agg(
            F.count(F.lit(1)).alias("rows"),
            fact_listens.collapsed_by_distinct(silver).alias("collapsed"),
        ).first()
        silver_rows = counted["rows"]
        collapsed = counted["collapsed"] or 0
        prepared = fact_listens.prepare(silver, built_at=built_at)

        print(f"Silver 행 : {silver_rows:,}")
        print(f"배열 안 중복 : {collapsed:,}개  "
              f"(array_distinct가 지웠다. 그대로 펼치면 grain 불변식이 깨진다)")

        # 1) 자리를 먼저 만든다
        added = 0
        if not args.skip_placeholders:
            missing = fact_listens.missing_artists(prepared, dim)
            added = missing.count()
            if added:
                rows = fact_listens.placeholders(missing, dim)
                rows.write.format("delta").mode("append").save(dim_path)
                dim = spark.read.format("delta").load(dim_path)
            print(f"차원에 없던 아티스트 : {added:,}  (자리를 만들었다)")
        else:
            print("자리를 만들지 않는다 (--skip-placeholders)")

        # 2) 팩트를 쓴다
        fact_listens.write(prepared, fact_path, silver_version=silver_version)
        elapsed = time.time() - started

        written = spark.read.format("delta").load(fact_path)
        fact_rows = written.count()
        listens = written.select(*fact.LISTEN_KEY).distinct().count()
        with_artist = written.filter(F.col("artist_mbid").isNotNull()).count()
        months = written.select(fact.PARTITION_COLUMN).distinct().count()

        print()
        print(f"소요      : {elapsed:.0f}초")
        print(f"팩트 행   : {fact_rows:,}")
        print(f"청취      : {listens:,}  (DISTINCT _source_dump, _record_id)")
        print(f"부풀림    : {fact_rows / listens:.4f}배")
        print(f"아티스트 있음 : {with_artist:,}  "
              f"({with_artist / fact_rows * 100:.2f}%)")
        print(f"파티션(달): {months:,}개")

        # 3) Silver 청취가 하나도 안 빠졌는지 대조한다.
        #    explode를 잘못 쓰면 여기서 걸린다.
        print()
        if listens == silver_rows:
            print(f"대조 통과 : Silver {silver_rows:,} = 팩트의 청취 {listens:,}")
        else:
            print(f"대조 실패 : Silver {silver_rows:,} != 팩트의 청취 {listens:,}  "
                  f"(차이 {silver_rows - listens:,})")

        checks = fact_listens.invariants(spark, fact_path, dim_path)
        print()
        print("팩트 불변식")
        labels = {
            "artists_not_in_dim": "차원에 없는 아티스트",
            "duplicate_grain_keys": "grain 키가 겹치는 줄",
            "rows_without_partition": "파티션 값이 없는 줄",
        }
        for name, value in checks.items():
            mark = "통과" if value == 0 else f"위반 {value:,}건"
            print(f"  {labels[name]:24} {mark}")

        config.write_measurement("fact-listens", {
            "built_at": built_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "silver_rows": silver_rows,
            "artist_array_duplicates": collapsed,
            "fact_rows": fact_rows,
            "distinct_listens": listens,
            "fan_out": round(fact_rows / listens, 4),
            "rows_with_artist": with_artist,
            "placeholders_added": added,
            "partitions": months,
            "invariants": checks,
        })

        if listens != silver_rows or any(checks.values()):
            print()
            print("실패")
            return 1
        print()
        print("통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(job_runs.run(main))
