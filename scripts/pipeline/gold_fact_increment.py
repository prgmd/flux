"""팩트를 Silver의 지금 버전까지 따라가게 한다. 전체를 다시 만들지 않는다.

팩트 커밋에 적어 둔 처리 위치(어느 Silver 버전까지 반영했나) 다음부터 Silver의 변경
기록을 읽는다. 빠진 청취의 줄을 지우고 들어온 청취의 줄을 넣는 병합 한 번이고, 새
처리 위치도 그 커밋에 적는다(flux.gold.fact_listens.increment).

Silver를 통째로 다시 만든 뒤에는 이어 갈 수 없다. 그때는 gold_fact_listens.py로 팩트를
다시 만든다. 잡이 그것을 알아보고 멈춘다.

    ./scripts/spark_run.sh -m 12g -- python scripts/pipeline/gold_fact_increment.py
"""

import argparse
import sys
import time
from datetime import datetime, timezone

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import fact, fact_listens, scd2
from flux.silver import schema as silver_schema


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    args = parser.parse_args()

    silver_path = config.silver(silver_schema.TABLE_NAME)
    fact_path = config.gold(fact.TABLE_NAME)
    dim_path = config.gold(scd2.TABLE_NAME)
    built_at = datetime.now(timezone.utc)

    spark = spark_session("flux-gold-fact-increment",
                          shuffle_partitions=args.shuffle_partitions)
    started = time.time()
    try:
        try:
            result = fact_listens.increment(spark, silver_path, fact_path, dim_path,
                                            built_at=built_at)
        except fact_listens.FullRebuildNeeded as reason:
            print(f"증분으로 이어 갈 수 없다: {reason}")
            return 2
        elapsed = time.time() - started

        print(f"Silver 버전 : {result['silver_from']} → {result['silver_to']}")
        print(f"소요        : {elapsed:.0f}초")
        if not result["applied"]:
            print("새로 반영할 것이 없다")
        else:
            print(f"지움        : {result['numTargetRowsDeleted']:,}")
            print(f"넣음        : {result['numTargetRowsInserted']:,}")
            print(f"자리 만듦   : {result['placeholdersAdded']:,}")
            print(f"파일        : 지움 {result['numTargetFilesRemoved']:,}, "
                  f"새로 씀 {result['numTargetFilesAdded']:,}")

        config.write_measurement(f"fact-increment-{result['silver_to']}", {
            "built_at": built_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            **result,
        })
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
