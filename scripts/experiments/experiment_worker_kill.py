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

    ./scripts/spark_run.sh -m 6g --name flux-worker-kill --network flux_default -p 4040:4040 \\
      -e FLUX_SPARK_MASTER=spark://spark-master:7077 \\
      -- python scripts/experiments/experiment_worker_kill.py --phase baseline

    # --phase killed로 다시 띄우고, 다른 창에서 죽일 순간을 규칙이 고르게 한다
    PYTHONPATH=src python scripts/experiments/kill_worker_mid_shuffle.py \\
      --container flux-spark-worker-2

죽이는 것은 **밖에서** 한다. 잡이 스스로를 죽이면 그건 장애가 아니라 기능이다.

## 순서

    1. baseline   워커를 안 죽이고 답을 낸다. 정답의 기준이다
    2. killed     같은 잡을 돌리는 도중에 워커 하나를 죽인다
    3. 대조        두 답이 같은가

2번의 답이 1번과 같아야 **재계산이 제대로 됐다**는 뜻이다. 잡이 죽지 않고
끝나기만 한 것으로는 부족하다. 조각을 잃고도 **맞는 답**을 내야 한다.

## 답이 같다는 것만으로는 모자란다

처음 짠 판정은 답 넷이 같은지만 봤다. 그러면 잡이 끝난 뒤에 죽였거나, 죽인
워커가 맡은 일이 없었어도 "통과"가 찍힌다. 게이트는 "재계산이 **일어나고**
결과가 정합한지"인데 앞쪽 절반을 확인하지 않은 것이다.

그래서 이벤트 로그를 남기고, 사라진 실행기와 그 실행기가 끝낸 태스크, 실패한
태스크와 다시 돈 단계를 센다(flux.common.eventlog.recovery). 기준선에서는
전부 0이어야 하고, 종료 실행에서는 잃은 것과 다시 한 것이 둘 다 있어야 한다.

## 언제 죽이는가

셔플 출력은 그것을 쓴 실행기의 디스크에 있다. **첫 셔플 단계가 끝난 뒤에**
죽여야 이미 끝난 일을 잃는다. 그 전에 죽이면 실행 중이던 태스크만 다시 돌고,
잃은 출력을 계보로 다시 만드는 장면은 안 나온다.

그 시점을 밖에서 보려고 이 실험에서만 UI를 켠다(4040). scripts/experiments/kill_worker_mid_shuffle.py가
UI의 REST API로 단계 상태를 보다가 그때 죽인다.
"""

import argparse
import json
import shutil
import sys
import time

from pyspark.sql import functions as F

from flux.common import config, eventlog
from flux.common.spark import master, session as spark_session
from flux.gold import fact, mart_build

UI_PORT = 4040

PHASES = ("baseline", "killed")


def workload(spark, fact_path: str) -> dict:
    """죽일 시간을 벌 만큼 무겁고, 답이 하나로 떨어지는 일.

    집계를 고른 이유가 있다. 답이 숫자 몇 개라 대조가 쉽고, 셔플이 있어
    중간 결과가 워커에 흩어져 **죽일 것이 실제로 있다.**
    """
    built = mart_build.build(spark, fact_path, salting=False)
    row = built.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum("listens").alias("listens"),
        F.countDistinct("artist_mbid").alias("artists"),
        # 합계만 보면 순서가 달라져도 같으므로 놓치는 오류가 있다.
        # 해시의 합은 어느 행 하나가 달라져도 바뀐다.
        mart_build.fingerprint("artist_mbid", "listened_date", "listens")
        .alias("fingerprint"),
    ).first()
    return {
        "rows": row["rows"],
        "listens": row["listens"],
        "artists": row["artists"],
        "fingerprint": str(row["fingerprint"]),
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
    name = f"worker-kill-{args.phase}"
    logs = config.local_root() / "eventlogs" / name
    if logs.exists():
        shutil.rmtree(logs)
    logs.mkdir(parents=True)

    print(f"마스터 : {master()}")
    print(f"단계   : {args.phase}")
    if args.phase == "killed":
        print("다른 창에서 scripts/experiments/kill_worker_mid_shuffle.py가 죽인다")
    print()

    spark = spark_session(
        name, shuffle_partitions=args.shuffle_partitions,
        extra={
            "spark.eventLog.enabled": "true",
            "spark.eventLog.dir": logs.as_uri(),
            "spark.ui.enabled": "true",
            "spark.ui.port": str(UI_PORT),
        })
    started = time.time()
    try:
        result = workload(spark, fact_path)
        elapsed = round(time.time() - started, 1)
    finally:
        spark.stop()

    rec = eventlog.recovery(eventlog.lines(logs))

    print(f"소요       : {elapsed}초")
    print(f"마트 행    : {result['rows']:,}")
    print(f"합계 청취  : {result['listens']:,}")
    print(f"아티스트   : {result['artists']:,}")
    print(f"지문       : {result['fingerprint']}")
    print()
    print(f"사라진 실행기     : {rec['executors_removed'] or '없음'}")
    print(f"그 실행기가 끝낸 일 : {rec['tasks_done_by_removed']}태스크")
    print(f"실패한 태스크     : {rec['failed_tasks'] or '없음'}")
    print(f"재시도로 성공     : {rec['retried_tasks']}태스크")
    print(f"다시 돈 단계      : {rec['resubmitted_stages'] or '없음'}")

    config.write_measurement(name, {
        "phase": args.phase,
        "master": master(),
        "shuffle_partitions": args.shuffle_partitions,
        "elapsed_seconds": elapsed,
        **result,
        "recovery": rec,
    })

    if args.phase == "baseline":
        if rec["executors_removed"] or rec["failed_tasks"]:
            print()
            print("기준선에서 실행기를 잃었다. 기준선으로 쓸 수 없다")
            return 1
        return 0

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

    if not eventlog.recomputed(rec):
        print()
        print("판정 불가: 재계산이 일어나지 않았다. 죽인 시점이나 대상을 확인한다")
        return 1
    if not all(same.values()):
        print()
        print("실패: 워커를 잃은 뒤 결과가 달라졌다")
        return 1
    print()
    print("통과: 워커를 잃고 재계산했고, 같은 답을 냈다")
    return 0


if __name__ == "__main__":
    sys.exit(main())
