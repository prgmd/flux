"""덤프 하나를 Silver에 붙인다. 전체를 다시 만들지 않는다.

새 덤프의 후보와, 같은 중복 제거 키로 Silver에 남아 있는 줄만 모아 남길 줄을 다시
고른다. 1등이 새 덤프에서 나온 키만 고치거나 넣는다(flux.silver.build.increment).
격리 줄은 그 덤프 몫만 갈아 끼운다.

같은 덤프를 다시 붙여도 결과가 같다. 번호가 줄 내용의 해시라 같은 줄이 다시 이기고,
병합은 (덤프, 번호)가 같은 줄을 건드리지 않는다.

    ./scripts/spark_run.sh -m 12g -- python scripts/pipeline/silver_increment.py --dump <덤프 이름>
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.common.spark import session as spark_session
from flux.silver import build, schema


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", required=True)
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--driver-memory", default="8g")
    args = parser.parse_args()

    bronze_path = config.bronze(bronze_schema.TABLE_NAME)
    silver_path = config.silver(schema.TABLE_NAME)
    quarantine_path = config.silver(schema.QUARANTINE_TABLE_NAME)
    built_at = datetime.now(timezone.utc)

    spark = spark_session("flux-silver-increment",
                          shuffle_partitions=args.shuffle_partitions,
                          memory=args.driver_memory)
    started = time.time()
    try:
        changes, rejected = build.increment(spark, bronze_path, silver_path,
                                            dump=args.dump, built_at=built_at)
        metrics = build.merge(spark, changes, silver_path)
        build.replace_quarantine(rejected, quarantine_path, dump=args.dump)
        elapsed = time.time() - started

        print(f"덤프       : {args.dump}")
        print(f"소요       : {elapsed:.0f}초")
        print(f"바꿀 줄    : {metrics['numSourceRows']:,}")
        print(f"  넣음     : {metrics['numTargetRowsInserted']:,}")
        print(f"  고침     : {metrics['numTargetRowsUpdated']:,}")
        print(f"파일       : 지움 {metrics['numTargetFilesRemoved']:,}, "
              f"새로 씀 {metrics['numTargetFilesAdded']:,}")

        config.write_measurement(f"silver-increment-{args.dump}", {
            "dump": args.dump,
            "built_at": built_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "merge": metrics,
        })
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
