"""워커를 하나 죽였을 때 재계산이 일어나고 결과가 정합한지 본다.

명세 S4 게이트다.

    워커 1개를 강제 종료했을 때 재계산이 일어나고 결과가 정합한지 확인한다

## 지금까지의 강제 종료와 무엇이 다른가

Bronze 적재와 SCD2 병합에서도 컨테이너를 죽여 봤다. 그때 확인한 것은
**쓰기의 원자성**이었다 — 커밋이 없으면 아무 일도 안 일어난다.

여기서 보는 것은 다르다. **계산 도중에 일꾼이 사라져도 답이 맞는가**다.

Spark의 데이터프레임은 결과가 아니라 **계산 방법**이다. 어떤 입력에서 어떤
변환을 거쳐 나왔는지가 계보(lineage)로 남아 있어, 중간 결과를 들고 있던
워커가 죽으면 **그 조각만 처음부터 다시 만든다.** 전체를 다시 하지 않는다.

이것이 분산 처리가 장애를 다루는 기본 방식이고, 체크포인트를 찍어 두는 방식과
대비된다. 다시 만들 수 있으면 저장할 필요가 없다.

## 로컬 모드로는 확인할 수 없다

`local[*]`에는 워커가 없다. 드라이버 하나가 스레드로 병렬 처리할 뿐이라
**죽일 대상이 없다.** compose의 standalone 클러스터에 붙어야 한다.

    docker compose up -d spark-master spark-worker

    docker run --rm --network flux_default \\
      -e FLUX_SPARK_MASTER=spark://spark-master:7077 \\
      -e FLUX_STORAGE_ROOT=file:///data ... flux-spark \\
      python scripts/experiment_worker_kill.py --phase baseline

    # 다른 창에서 잡이 도는 중에
    docker kill flux-spark-worker-2

죽이는 것은 **밖에서** 한다. 잡이 스스로를 죽이면 그건 장애가 아니라 기능이다.

## 순서

    1. baseline   워커를 안 죽이고 답을 낸다. 정답의 기준이다
    2. killed     같은 잡을 돌리는 도중에 워커 하나를 죽인다
    3. 대조        두 답이 같은가

2번의 답이 1번과 같아야 **재계산이 제대로 됐다**는 뜻이다. 잡이 죽지 않고
끝나기만 한 것으로는 부족하다. 조각을 잃고도 **맞는 답**을 내야 한다.
"""

import argparse
import json
import sys
import time

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import master, session as spark_session
from flux.gold import fact, mart

PHASES = ("baseline", "killed")


def workload(spark, fact_path: str) -> dict:
    """죽일 시간을 벌 만큼 무겁고, 답이 하나로 떨어지는 일.

    집계를 고른 이유가 있다. 답이 숫자 몇 개라 대조가 쉽고, 셔플이 있어
    중간 결과가 워커에 흩어져 **죽일 것이 실제로 있다.**
    """
    built = mart.build(spark, fact_path, salting=False)
    row = built.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("listens").alias("listens"),
        F.countDistinct("artist_mbid").alias("artists"),
        # 합계만 보면 순서가 달라져도 같으므로 놓치는 오류가 있다.
        # 해시의 합은 어느 행 하나가 달라져도 바뀐다.
        F.sum(F.xxhash64("artist_mbid", "listened_date", "listens"))
         .alias("fingerprint"),
    ).first()
    return {
        "rows": row["rows"],
        "listens": row["listens"],
        "artists": row["artists"],
        "fingerprint": row["fingerprint"],
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--phase", choices=PHASES, required=True)
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    args = parser.parse_args()

    if master().startswith("local"):
        print(f"마스터가 {master()}다. 로컬 모드에는 워커가 없어 죽일 수 없다.")
        print("FLUX_SPARK_MASTER=spark://spark-master:7077로 붙어서 돌린다.")
        return 1

    fact_path = config.gold(fact.TABLE_NAME)
    spark = spark_session(f"flux-worker-kill-{args.phase}",
                          shuffle_partitions=args.shuffle_partitions)
    started = time.time()
    try:
        print(f"마스터 : {master()}")
        print(f"단계   : {args.phase}")
        if args.phase == "killed":
            print("지금 다른 창에서 워커 하나를 죽인다:")
            print("  docker kill flux-spark-worker-2")
        print()

        result = workload(spark, fact_path)
        elapsed = round(time.time() - started, 1)

        print(f"소요       : {elapsed}초")
        print(f"마트 행    : {result['rows']:,}")
        print(f"합계 청취  : {result['listens']:,}")
        print(f"아티스트   : {result['artists']:,}")
        print(f"지문       : {result['fingerprint']}")

        name = f"worker-kill-{args.phase}"
        config.write_measurement(name, {
            "phase": args.phase,
            "master": master(),
            "shuffle_partitions": args.shuffle_partitions,
            "elapsed_seconds": elapsed,
            **result,
        })

        # baseline이 있으면 바로 대조한다.
        if args.phase == "killed":
            base_path = (config.local_root() / config.MEASUREMENTS
                         / "worker-kill-baseline.json")
            if not base_path.exists():
                print()
                print("기준선이 없다. --phase baseline을 먼저 돌린다.")
                return 1
            base = json.loads(base_path.read_text(encoding="utf-8"))
            same = {k: base[k] == result[k] for k in
                    ("rows", "listens", "artists", "fingerprint")}
            print()
            print("기준선과 대조")
            for key, ok in same.items():
                print(f"  {key:12} {base[key]:>22}  "
                      f"{'일치' if ok else '불일치 ' + str(result[key])}")
            slower = (elapsed / base["elapsed_seconds"]
                      if base.get("elapsed_seconds") else 0)
            print()
            print(f"재계산 대가 : {slower:.2f}배 "
                  f"({base['elapsed_seconds']}초 → {elapsed}초)")
            if not all(same.values()):
                print()
                print("실패: 워커를 잃은 뒤 결과가 달라졌다")
                return 1
            print()
            print("통과: 워커를 잃고도 같은 답을 냈다")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
