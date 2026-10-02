"""차원 이름 조인을 세 방식으로 잰다. 명세 6.5의 broadcast 대 shuffle 줄이다.

    shuffle     양쪽을 아티스트 키로 셔플해 정렬 병합한다
    broadcast   차원 전체(297만 버전)를 실행기마다 복사한다
    pruned      마트에 나오는 아티스트만 남긴 차원을 복사한다

## 왜 gold_mart.py로 재지 않는가

`gold_mart.py --no-broadcast`로 재면 한 번 돌 때마다 집계(groupBy)와 마트 쓰기가
같이 돈다. 재려는 것은 이름 조인인데 시간에 그 둘이 섞이고, 운영 마트를 매번
덮어쓴다. 그래서 이미 만든 마트에서 이름을 떼어 입력으로 쓰고, **이름 조인만** 돌린다.

## 세 번째 안이 있는 이유

차원은 297만 버전인데 마트에 나오는 아티스트는 31만이다. 나머지 9할은 어느 쪽으로
조인하든 옮기기만 하고 버려진다. 조인 전에 쓰이는 키만 남기면 옮길 것이 준다. 대신
쓰이는 키를 알려면 마트를 한 번 더 읽어야 한다. 그 값이 남는 장사인지를 잰다.

## 메모리

명세가 이 줄에 적은 지표는 실행 시간과 실행기 메모리다. 메모리는 이벤트 로그에서
태스크가 끝날 때마다 남는 실행기 지표의 최고값으로 본다(flux.common.eventlog.peak_memory).
브로드캐스트된 차원은 블록 매니저의 저장 메모리에 올라가므로 `storage`가 그 비용을
보여준다. 힙 전체(`jvm_heap`)는 GC 시점에 따라 흔들려서 함께 적되 무게를 덜 둔다.

## 셋이 같은 답을 내는지

행 수, 이름이 붙은 행 수, 그리고 `(아티스트, 날짜, 이름)`의 해시 합을 비교한다.
해시 합은 어느 행 하나의 이름만 달라도 바뀐다.

    ./scripts/spark_run.sh -m 10g -- python scripts/experiment_broadcast.py
"""

import argparse
import json
import shutil
import sys
import time

from pyspark.sql import functions as F

from flux.common import config, eventlog
from flux.common.spark import master as spark_master
from flux.common.spark import session as spark_session
from flux.gold import mart, mart_build, scd2

VARIANTS = ("shuffle", "broadcast", "pruned")
REPEATS = 3
JOINS = ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin",
         "BroadcastNestedLoopJoin")


def mb(value) -> str:
    return "못 잼" if not value else f"{value / 1e6:.1f}MB"


def names_join(spark, mart_path: str, dim_path: str, variant: str):
    unnamed = spark.read.format("delta").load(mart_path).drop("artist_name")
    dim = spark.read.format("delta").load(dim_path)
    if variant == "pruned":
        used = unnamed.select(scd2.BUSINESS_KEY).distinct()
        dim = dim.join(F.broadcast(used), on=scd2.BUSINESS_KEY, how="left_semi")
    return mart_build.with_names(unnamed, dim,
                                 broadcast=variant != "shuffle")


def from_logs(logs, plan_path) -> dict:
    """이벤트 로그에서 계획, 셔플, 메모리를 꺼낸다. 실행 직후에도, 나중에 다시도 쓴다.

    계획은 처음엔 쿼리 객체에 `explain`을 걸어 뽑았다. 그런데 `first()`는 내부에서
    `limit(1)`을 붙인 **새 쿼리**를 돌리므로, 계획을 뽑은 원래 쿼리는 실행된 적이 없었다.
    아홉 런의 계획이 전부 `isFinalPlan=false`였다. 실제로 돈 계획은 이벤트 로그에 있다.
    """
    plan = eventlog.join_plan(eventlog.lines(logs)) or ""
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(plan, encoding="utf-8")
    summary = eventlog.summary(eventlog.read(logs))
    return {
        "joins": {j: plan.count(j) for j in JOINS if plan.count(j)},
        "final_plan": "isFinalPlan=true" in plan,
        "shuffle_read_bytes": summary["shuffle_read_bytes"],
        "shuffle_write_bytes": summary["shuffle_write_bytes"],
        "total_task_ms": summary["total_task_ms"],
        "stages": summary["stages"],
        "memory": eventlog.peak_memory(eventlog.lines(logs)),
    }


def run_once(variant: str, rep: int, *, mart_path: str, dim_path: str,
             out: str, shuffle_partitions: int) -> dict:
    root = config.local_root()
    name = f"{variant}-{rep}"
    logs = root / "eventlogs" / out / name
    if logs.exists():
        shutil.rmtree(logs)
    logs.mkdir(parents=True)

    spark = spark_session(f"flux-broadcast-{name}",
                          shuffle_partitions=shuffle_partitions,
                          extra={"spark.eventLog.enabled": "true",
                                 "spark.eventLog.dir": logs.as_uri(),
                                 "spark.executor.metrics.pollingInterval": "100"})
    try:
        started = time.time()
        named = names_join(spark, mart_path, dim_path, variant)
        query = named.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col("artist_name").isNotNull().cast("long")).alias("named"),
            mart_build.fingerprint("artist_mbid", "listened_date", "artist_name")
            .alias("fingerprint"),
        )
        got = query.first()
        elapsed = time.time() - started
        aqe = spark.conf.get("spark.sql.adaptive.enabled")
    finally:
        spark.stop()

    plan_path = root / config.MEASUREMENTS / "plans" / out / f"{name}.txt"
    return {
        "variant": variant,
        "repeat": rep,
        "wall_seconds": round(elapsed, 1),
        "rows": got["rows"],
        "named": got["named"],
        "fingerprint": str(got["fingerprint"]),
        "aqe": aqe,
        **from_logs(logs, plan_path),
    }


def rederive(out: str) -> list:
    """저장된 실측 파일의 런마다 이벤트 로그를 다시 읽어 로그에서 나온 값을 갈아 끼운다.

    소요 시간과 답(행 수, 지문)은 실행 중에만 알 수 있으므로 그대로 둔다.
    """
    root = config.local_root()
    saved = json.loads((root / config.MEASUREMENTS / f"{out}.json")
                       .read_text(encoding="utf-8"))
    runs = []
    for run in saved["runs"]:
        name = f"{run['variant']}-{run['repeat']}"
        logs = root / "eventlogs" / out / name
        plan_path = root / config.MEASUREMENTS / "plans" / out / f"{name}.txt"
        runs.append({**run, **from_logs(logs, plan_path)})
    return runs


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=REPEATS)
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--out", default="broadcast-experiment")
    parser.add_argument("--rederive", action="store_true",
                        help="돌리지 않고 저장된 이벤트 로그에서 다시 뽑는다")
    args = parser.parse_args()

    mart_path = config.gold(mart.DAILY_ARTIST)
    dim_path = config.gold(scd2.TABLE_NAME)
    print(f"마스터 {spark_master()}, 회차 {args.repeats}, 실측 파일 {args.out}")
    print()

    results = rederive(args.out) if args.rederive else []
    for rep in range(1, 0 if args.rederive else args.repeats + 1):
        for variant in VARIANTS:
            row = run_once(variant, rep, mart_path=mart_path,
                           dim_path=dim_path, out=args.out,
                           shuffle_partitions=args.shuffle_partitions)
            results.append(row)
            print(f"  {rep}회차 {variant:9} {row['wall_seconds']:7.1f}초  "
                  f"셔플 r {row['shuffle_read_bytes'] / 1e6:8.1f}MB  "
                  f"w {row['shuffle_write_bytes'] / 1e6:8.1f}MB  "
                  f"저장 {mb(row['memory']['storage']):>7}  "
                  f"힙 {mb(row['memory']['jvm_heap']):>7}  "
                  f"{row['joins']}", flush=True)

    print()
    print(f"{'방식':10} {'중앙 소요':>9} {'최소':>7} {'최대':>7} "
          f"{'셔플 read':>11} {'셔플 write':>11} {'저장 최고':>10} "
          f"{'힙 최고':>9}  조인")
    table = []
    for variant in VARIANTS:
        rows = [r for r in results if r["variant"] == variant]
        walls = sorted(r["wall_seconds"] for r in rows)
        storage = sorted(r["memory"]["storage"] or 0 for r in rows)
        heap = sorted(r["memory"]["jvm_heap"] or 0 for r in rows)
        entry = {
            "variant": variant,
            "runs": len(rows),
            "wall_median": walls[len(walls) // 2],
            "wall_min": walls[0],
            "wall_max": walls[-1],
            "shuffle_read_bytes": rows[0]["shuffle_read_bytes"],
            "shuffle_write_bytes": rows[0]["shuffle_write_bytes"],
            "joins": rows[0]["joins"],
            "storage_peak_median": storage[len(storage) // 2],
            "heap_peak_median": heap[len(heap) // 2],
        }
        table.append(entry)
        print(f"{variant:10} {entry['wall_median']:8.1f}초 "
              f"{entry['wall_min']:6.1f}초 {entry['wall_max']:6.1f}초 "
              f"{entry['shuffle_read_bytes'] / 1e6:9.1f}MB "
              f"{entry['shuffle_write_bytes'] / 1e6:9.1f}MB "
              f"{mb(entry['storage_peak_median']):>10} "
              f"{mb(entry['heap_peak_median']):>9}  {entry['joins']}")

    answers = sorted({(r["rows"], r["named"], r["fingerprint"]) for r in results})
    agreed = len(answers) == 1
    print()
    if agreed:
        rows, named, fp = answers[0]
        print(f"대조 통과 : 세 방식이 같은 답을 냈다 ({rows:,}행, 이름 {named:,}, 지문 {fp})")
    else:
        print(f"대조 실패 : {answers}")

    config.write_measurement(args.out, {
        "master": spark_master(),
        "shuffle_partitions": args.shuffle_partitions,
        "repeats": args.repeats,
        "runs": results,
        "summary": table,
        "agreed": agreed,
    })
    return 0 if agreed else 1


if __name__ == "__main__":
    sys.exit(main())
