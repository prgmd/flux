"""user_id가 사용자를 안정적으로 가리키는지 전수로 확인한다.

중복 조사에서 표본 400개 중 294개(73.5%)가 user_name은 같은데 user_id가 달랐다.
사실이면 명세 5.3의 Gold 팩트가 user_id를 dim_user FK로 쓰는 설계가 성립하지 않는다.
같은 사람이 여러 dim 행에 걸리기 때문이다.

세 가지를 본다.

1. user_name 하나에 user_id가 여럿인가          (사용자가 갈라지는가)
2. user_id 하나에 user_name이 여럿인가          (번호가 재사용되는가)
3. 한 덤프 안에서도 흔들리는가                   (덤프 사이의 문제인가 아닌가)

cache()를 쓰지 않는다. 집계마다 다시 훑는 편이 메모리에 올리는 것보다 안전하다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/measure_user_identity.py
"""

import argparse
import sys

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session

BRONZE_TABLE = "raw_listens"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    args = parser.parse_args()

    spark = spark_session("flux-user-identity", shuffle_partitions=args.shuffle_partitions)
    try:
        bronze = spark.read.format("delta").load(config.bronze(BRONZE_TABLE))
        pairs = bronze.select(
            F.col("user_name"),
            F.get_json_object("raw_json", "$.user_id").alias("user_id"),
            F.col("_source_dump"),
        )

        missing = pairs.filter(F.col("user_id").isNull()).count()
        print(f"user_id가 없는 행 : {missing:,}")

        # 1. 이름 하나에 번호가 여럿인가
        by_name = pairs.groupBy("user_name").agg(
            F.countDistinct("user_id").alias("ids"))
        name_summary = by_name.agg(
            F.count(F.lit(1)).alias("names"),
            F.count(F.when(F.col("ids") > 1, 1)).alias("split"),
            F.max("ids").alias("max_ids"),
        ).collect()[0]

        names = name_summary["names"]
        split = name_summary["split"]
        print()
        print(f"서로 다른 user_name : {names:,}")
        print(f"  번호가 여럿인 이름 : {split:,}  ({split / names * 100:.2f}%)")
        print(f"  한 이름의 최대 번호 수 : {name_summary['max_ids']:,}")

        # 2. 번호 하나에 이름이 여럿인가
        by_id = pairs.groupBy("user_id").agg(
            F.countDistinct("user_name").alias("names"))
        id_summary = by_id.agg(
            F.count(F.lit(1)).alias("ids"),
            F.count(F.when(F.col("names") > 1, 1)).alias("shared"),
            F.max("names").alias("max_names"),
        ).collect()[0]

        ids = id_summary["ids"]
        shared = id_summary["shared"]
        print()
        print(f"서로 다른 user_id : {ids:,}")
        print(f"  이름이 여럿인 번호 : {shared:,}  ({shared / ids * 100:.2f}%)")
        print(f"  한 번호의 최대 이름 수 : {id_summary['max_names']:,}")

        # 3. 한 덤프 안에서도 흔들리는가
        within = (
            pairs.groupBy("user_name", "_source_dump")
                 .agg(F.countDistinct("user_id").alias("ids"))
                 .filter(F.col("ids") > 1)
                 .count()
        )
        print()
        print(f"한 덤프 안에서도 번호가 여럿인 (이름, 덤프) : {within:,}")

        examples = (
            by_name.filter(F.col("ids") > 1)
                   .orderBy(F.col("ids").desc())
                   .limit(5).collect()
        )
        if examples:
            print()
            print("번호가 가장 많이 갈린 이름")
            for row in examples:
                sample = (
                    pairs.filter(F.col("user_name") == row["user_name"])
                         .select("user_id", "_source_dump").distinct()
                         .orderBy("_source_dump").limit(4).collect()
                )
                print(f"  {row['user_name']} : 번호 {row['ids']}개")
                for s in sample:
                    print(f"      {s['user_id']:>10}  {s['_source_dump'][-30:]}")

        payload = {
            "rows_missing_user_id": missing,
            "distinct_user_names": names,
            "names_with_multiple_ids": split,
            "max_ids_per_name": name_summary["max_ids"],
            "distinct_user_ids": ids,
            "ids_with_multiple_names": shared,
            "max_names_per_id": id_summary["max_names"],
            "within_dump_conflicts": within,
        }
        config.write_measurement("user-identity", payload)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
