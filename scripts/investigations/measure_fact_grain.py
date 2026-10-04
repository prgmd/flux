"""팩트 한 줄이 무엇을 뜻할지 정하기 전에 잰다.

명세 5.3의 `fact_listens`는 `artist_mbid`를 **단수**로 적는다. 그런데 Silver는
`artist_mbids`가 **배열**이다. 한 청취에 아티스트가 여럿일 수 있기 때문이다.

이 차이를 메우는 방법이 넷이고, 어느 것이 맞는지는 **아티스트가 실제로 몇 명씩
붙어 있는가**에 달려 있다.

    펼치기      청취 하나가 아티스트 수만큼 줄이 된다. 줄 수가 부푼다
    배열 유지   줄 수는 그대로지만 조인할 때마다 펼쳐야 한다
    첫째만      협업 정보를 버린다
    브리지 표   팩트는 청취 단위로 두고 연결을 별도 표로 뺀다

재는 것은 셋이다.

    1. 아티스트 수 분포        펼치면 얼마나 부푸는가
    2. 서로 다른 아티스트 수   dim_artist와 견줄 크기인가
    3. dim에 없는 MBID        late-arriving dimension이 실재하는가

3번은 S3에서 "확인할 수 없다"고 되돌린 게이트 항목이다. 여기서 재면 placeholder
행을 몇 개 만들어야 하는지 알 수 있다.

    ./scripts/spark_run.sh -- python scripts/investigations/measure_fact_grain.py
"""

import sys
import time

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import scd2
from flux.silver import schema

BUCKETS = (0, 1, 2, 3, 4, 5)


def main() -> int:
    silver_path = config.silver(schema.TABLE_NAME)
    dim_path = config.gold(scd2.TABLE_NAME)

    spark = spark_session("flux-fact-grain", shuffle_partitions=200)
    started = time.time()
    try:
        silver = spark.read.format("delta").load(silver_path)
        sized = silver.withColumn(
            "_n", F.coalesce(F.size(F.col("artist_mbids")), F.lit(0)))

        total = sized.count()
        # 한 번에 세서 통과를 줄인다. 배열 길이별 행 수와 전체 원소 수.
        summary = sized.agg(
            F.sum(F.col("_n")).alias("elements"),
            *[F.sum((F.col("_n") == n).cast("long")).alias(f"n{n}")
              for n in BUCKETS],
            F.sum((F.col("_n") > max(BUCKETS)).cast("long")).alias("more"),
            F.max("_n").alias("max_n"),
        ).first()

        elements = summary["elements"]
        empty = summary["n0"]
        outer = elements + empty

        print(f"Silver 행      : {total:,}")
        print(f"아티스트 원소 수: {elements:,}")
        print(f"한 청취 최대   : {summary['max_n']}명")
        print()
        # explode와 explode_outer의 차이가 여기서 갈린다. 앞엣것은 배열이
        # 비었거나 NULL인 행을 결과에서 없앤다. 두 값을 나란히 찍어야
        # "0.40배"가 부풀림이 아니라 삭제라는 것이 보인다.
        print(f"explode        : {elements:,}행  ({elements / total:.4f}배)  "
              f"**{empty:,}행이 사라진다**")
        print(f"explode_outer  : {outer:,}행  ({outer / total:.4f}배)  "
              f"늘어난 줄 {outer - total:,}")
        print()
        print("아티스트 수 분포")
        for n in BUCKETS:
            count = summary[f"n{n}"]
            label = "없음" if n == 0 else f"{n}명"
            print(f"  {label:6} {count:>14,}  {count / total * 100:6.2f}%")
        print(f"  {max(BUCKETS)}명 초과 {summary['more']:>10,}  "
              f"{summary['more'] / total * 100:6.2f}%")

        # 서로 다른 아티스트와 dim 존재 여부
        used = (
            sized.filter(F.col("_n") > 0)
                 .select(F.explode("artist_mbids").alias("artist_mbid"))
                 .distinct()
        )
        distinct_used = used.count()

        dim_keys = (
            spark.read.format("delta").load(dim_path)
                 .select(scd2.BUSINESS_KEY).distinct()
        )
        dim_count = dim_keys.count()
        missing = used.join(dim_keys, on=scd2.BUSINESS_KEY, how="left_anti").count()

        print()
        print(f"청취에 쓰인 서로 다른 아티스트 : {distinct_used:,}")
        print(f"dim_artist의 아티스트          : {dim_count:,}")
        print(f"dim에 없는 아티스트            : {missing:,}  "
              f"({missing / distinct_used * 100:.2f}%)")

        elapsed = time.time() - started
        print()
        print(f"소요 : {elapsed:.0f}초")

        config.write_measurement("fact-grain", {
            "elapsed_seconds": round(elapsed, 1),
            "silver_rows": total,
            "artist_elements": elements,
            "listens_without_artist": empty,
            "explode_rows": elements,
            "explode_ratio": round(elements / total, 4),
            "explode_outer_rows": outer,
            "explode_outer_ratio": round(outer / total, 4),
            "max_artists": summary["max_n"],
            "artists_per_listen": {str(n): summary[f"n{n}"] for n in BUCKETS}
                                  | {"more": summary["more"]},
            "distinct_artists_used": distinct_used,
            "dim_artists": dim_count,
            "artists_missing_from_dim": missing,
        })
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
