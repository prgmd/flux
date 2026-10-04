"""비싼 조인 실험 앞에 같은 코드 경로를 한두 달치로만 태워 본다.

    ./scripts/spark_run.sh -m 6g [--network flux_default -e FLUX_SPARK_MASTER=...] \
        -- python scripts/experiments/precheck_join.py --months 1

experiment_skew.py 12런은 로컬에서 25분, 클러스터에서 50분이다. 마운트가 빗나갔거나
컬럼 이름이 틀렸거나 소금 경로가 메모리를 못 버티면, 그것을 50분 뒤에 아는 것은
비싸다. 같은 함수(mart_build.join_for_experiment)를 파티션 몇 개에만 돌려
두 변형이 같은 결과를 내는지까지 본다.

여기서 재는 시간은 측정이 아니다. 차원 쪽은 달 수와 무관하게 전부 읽으므로
전체 실행으로 늘려 잡을 수 없다.
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from flux.common import config, deltalog
from flux.common.spark import master, session as spark_session
from flux.gold import fact, mart_build, scd2


def recent_months(count: int) -> list:
    """최근 몇 달. **Spark 잡 없이 Delta 로그에서** 읽는다.

    처음엔 `select(listened_month).distinct()`로 뽑았다. 그러면 팩트의 활성 파일
    16,598개를 전부 열어 한 달치 확인이 전체 실행만큼(로컬 117초) 걸렸다. 파티션
    값은 로그의 `add` 행에 이미 적혀 있다(flux.common.deltalog.partition_values).
    """
    table = config.local_root() / config.GOLD / fact.TABLE_NAME
    values = deltalog.partition_values(table)
    return sorted({v[fact.PARTITION_COLUMN] for v in values.values()})[-count:]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--months", type=int, default=1,
                        help="최근 몇 달치만 태울지")
    parser.add_argument("--shuffle-partitions", type=int, default=24)
    args = parser.parse_args()

    fact_path = config.gold(fact.TABLE_NAME)
    dim_path = config.gold(scd2.TABLE_NAME)
    months = recent_months(args.months)
    print(f"마스터 {master()}, 달 {','.join(months)}")

    results = {}
    for salting in (False, True):
        name = "salted" if salting else "plain"
        spark = spark_session(f"flux-precheck-{name}",
                              shuffle_partitions=args.shuffle_partitions)
        try:
            started = time.time()
            dim = spark.read.format("delta").load(dim_path)
            built = mart_build.join_for_experiment(
                spark, fact_path, dim, salting=salting, months=months)
            got = built.agg(
                F.count(F.lit(1)).alias("rows"),
                F.sum(F.col("name").isNotNull().cast("long")).alias("named"),
            ).first()
            results[name] = (got["rows"], got["named"])
            print(f"{name:7} {got['rows']:>12,}행  "
                  f"이름 붙음 {got['named']:>12,}  {time.time() - started:.1f}초",
                  flush=True)
        except Exception as exc:
            print(f"{name:7} 실패 {type(exc).__name__} {exc}", flush=True)
            return 1
        finally:
            spark.stop()

    if results["plain"] != results["salted"]:
        print(f"불일치 {results}")
        return 1
    print("두 변형이 같은 결과를 냈다")
    return 0


if __name__ == "__main__":
    sys.exit(main())
