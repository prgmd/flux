"""일별 아티스트 마트를 만든다. 명세 S4의 집계 마트다.

팩트를 아티스트와 날짜로 묶는다. 스큐 실험이 재는 것도 이 작업이라
`--salting`으로 두 경로를 다 돌려볼 수 있다.

이름은 **집계 뒤에** 붙인다. 앞에서 붙이면 1.5억 줄에 시점 조인을 걸게 되고,
뒤에서 붙이면 아티스트-날짜 조합에만 붙는다. 같은 답인데 일의 양이 다르다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \\
        python scripts/gold_mart.py
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import fact, mart, scd2


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--salting", action="store_true",
                        help="소금을 쳐서 두 단계로 묶는다")
    parser.add_argument("--no-broadcast", action="store_true",
                        help="이름 조인에 브로드캐스트를 강제하지 않는다")
    parser.add_argument("--observe", action="store_true",
                        help="묶기 전에 치우침만 재고 끝낸다")
    args = parser.parse_args()

    fact_path = config.gold(fact.TABLE_NAME)
    dim_path = config.gold(scd2.TABLE_NAME)
    mart_path = config.gold(mart.DAILY_ARTIST)

    spark = spark_session("flux-gold-mart",
                          shuffle_partitions=args.shuffle_partitions)
    started = time.time()
    try:
        # 기법을 걸기 전에 병목이 실재하는지 본다. 없는 병목에 salting을
        # 걸면 소금값만 치르고 얻는 것이 없다.
        skewness = mart.observe_skew(spark, fact_path)
        print("아티스트별 청취 분포")
        print(f"  아티스트   : {skewness['keys']:,}")
        print(f"  청취       : {skewness['total']:,}")
        print(f"  최대       : {skewness['max']:,}")
        print(f"  중앙값     : {skewness['median']:,}")
        print(f"  99분위     : {skewness['p99']:,}")
        print(f"  최대/중앙  : {skewness['max_over_median']}배")
        print()
        print("  상위 아티스트")
        for row in skewness["top"][:5]:
            print(f"    {row['artist_mbid']}  {row['listens']:>10,}")

        config.write_measurement("mart-skew-observed", skewness)
        if args.observe:
            return 0

        print()
        built = mart.build(spark, fact_path, salting=args.salting)
        dim = spark.read.format("delta").load(dim_path)
        named = mart.with_names(built, dim, broadcast=not args.no_broadcast)
        (
            named.write.format("delta").mode("overwrite")
            .option("overwriteSchema", "true").save(mart_path)
        )
        elapsed = time.time() - started

        written = spark.read.format("delta").load(mart_path)
        rows = written.count()
        listens = written.agg(F.sum("listens")).first()[0]
        unnamed = written.filter(F.col("artist_name").isNull()).count()

        print(f"소요       : {elapsed:.0f}초  "
              f"(salting {'있음' if args.salting else '없음'})")
        print(f"마트 행    : {rows:,}")
        print(f"합계 청취  : {listens:,}")
        print(f"이름 없음  : {unnamed:,}")

        # 마트의 청취 합이 팩트의 아티스트 있는 줄 수와 같아야 한다.
        fact_rows = (
            spark.read.format("delta").load(fact_path)
            .filter(F.col("artist_mbid").isNotNull()).count()
        )
        print()
        if listens == fact_rows:
            print(f"대조 통과 : 마트 합계 {listens:,} = 팩트 줄 {fact_rows:,}")
        else:
            print(f"대조 실패 : 마트 합계 {listens:,} != 팩트 줄 {fact_rows:,}  "
                  f"(차이 {fact_rows - listens:,})")

        config.write_measurement("gold-mart", {
            "elapsed_seconds": round(elapsed, 1),
            "salting": args.salting,
            "broadcast": not args.no_broadcast,
            "shuffle_partitions": args.shuffle_partitions,
            "mart_rows": rows,
            "total_listens": listens,
            "rows_without_name": unnamed,
            "fact_rows_with_artist": fact_rows,
            "skew": skewness,
        })
        if listens != fact_rows:
            return 1
        print()
        print("통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
