"""중복 제거 키가 실제 데이터에서 어떻게 작동하는지 잰다.

명세 6.3의 키는 user_name + listened_at + recording_msid다. 코드로 옮기기 전에
세 가지를 확인한다.

1. 중복이 얼마나 되는가. 임포터가 79.4%인 데이터라 같은 이력을 두 번 가져왔을 수 있다
2. 중복끼리 **내용이 같은가.** 다르면 어느 쪽을 남길지 규칙이 필요하다
3. 키 컬럼에 빈 값이 있는가. 있으면 그 행들은 서로 구분되지 않는다

cache()를 쓰지 않는다. 1억 5571만 행을 메모리에 올리려다 한 번 죽었다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/measure_duplicates.py
"""

import argparse
import sys

from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.common.spark import session as spark_session


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    args = parser.parse_args()

    spark = spark_session("flux-measure-duplicates", shuffle_partitions=args.shuffle_partitions)
    try:
        bronze = spark.read.format("delta").load(config.bronze(bronze_schema.TABLE_NAME))

        keyed = bronze.select(
            F.col("user_name"),
            F.col("listened_at"),
            F.get_json_object("raw_json", "$.recording_msid").alias("msid"),
            F.col("_source_dump"),
            # 내용 비교는 원문 대신 지문으로 한다. 문자열을 그대로 셔플하면
            # 1억 5571만 개의 긴 문자열이 네트워크를 타야 한다.
            F.md5(F.col("raw_json")).alias("digest"),
        )

        total = bronze.count()
        nulls = keyed.agg(
            F.count(F.when(F.col("user_name").isNull(), 1)).alias("user_name"),
            F.count(F.when(F.col("listened_at").isNull(), 1)).alias("listened_at"),
            F.count(F.when(F.col("msid").isNull(), 1)).alias("msid"),
        ).collect()[0]

        print(f"전체 행 : {total:,}")
        print()
        print("키 컬럼의 빈 값")
        for name in ("user_name", "listened_at", "msid"):
            n = nulls[name]
            print(f"  {name:14} {n:>12,}  {n / total * 100:6.2f}%")

        groups = (
            keyed.groupBy("user_name", "listened_at", "msid")
                 .agg(F.count(F.lit(1)).alias("n"),
                      F.countDistinct("digest").alias("shapes"),
                      F.countDistinct("_source_dump").alias("dumps"))
        )

        summary = groups.agg(
            F.count(F.lit(1)).alias("keys"),
            F.count(F.when(F.col("n") > 1, 1)).alias("dup_keys"),
            F.sum(F.when(F.col("n") > 1, F.col("n") - 1).otherwise(0)).alias("dup_rows"),
            F.max("n").alias("max_n"),
            F.count(F.when(F.col("shapes") > 1, 1)).alias("differing_keys"),
            F.count(F.when((F.col("n") > 1) & (F.col("dumps") > 1), 1))
             .alias("cross_dump_keys"),
            F.count(F.when((F.col("n") > 1) & (F.col("dumps") == 1), 1))
             .alias("same_dump_keys"),
        ).collect()[0]

        keys = summary["keys"]
        dup_keys = summary["dup_keys"]
        dup_rows = summary["dup_rows"] or 0

        print()
        print(f"서로 다른 키 : {keys:,}")
        print(f"중복인 키    : {dup_keys:,}  ({dup_keys / keys * 100:.4f}%)")
        print(f"지워질 행    : {dup_rows:,}  ({dup_rows / total * 100:.4f}%)")
        print(f"한 키의 최대 : {summary['max_n']:,}행")

        print()
        print("중복의 성격")
        print(f"  내용이 서로 다른 키 : {summary['differing_keys']:,}")
        print(f"  덤프를 걸친 중복    : {summary['cross_dump_keys']:,}")
        print(f"  한 덤프 안의 중복   : {summary['same_dump_keys']:,}")

        payload = {
            "total_rows": total,
            "null_user_name": nulls["user_name"],
            "null_listened_at": nulls["listened_at"],
            "null_msid": nulls["msid"],
            "distinct_keys": keys,
            "duplicate_keys": dup_keys,
            "rows_removed": dup_rows,
            "max_rows_per_key": summary["max_n"],
            "keys_with_differing_content": summary["differing_keys"],
            "cross_dump_duplicate_keys": summary["cross_dump_keys"],
            "same_dump_duplicate_keys": summary["same_dump_keys"],
        }
        config.write_measurement("duplicates", payload)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
