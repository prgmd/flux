"""명세 6.4의 4단계 비교 실험. 이 프로젝트의 최종 산출물이다.

    1  AQE 비활성                    병목이 그대로 드러난 기준선
    2  AQE 활성                      엔진 자동 최적화
    3  AQE 비활성 + 수동 Salting      수동 기법의 효과
    4  AQE 활성 + Salting            둘 다

구성마다 3회씩, 모두 12런이다.

## 순서를 섞는 이유

구성별로 3회씩 몰아서 돌리면 안 된다. 같은 데이터를 반복해 읽으므로 뒤로 갈수록
OS 페이지 캐시가 더워져 **나중에 돈 구성이 유리해진다.** 그러면 측정한 것이
기법의 효과가 아니라 실행 순서가 된다.

그래서 **회차 단위로 돌린다.** 1회차에 네 구성을 한 번씩, 2회차에 다시 한 번씩.
캐시의 이득이 네 구성에 고르게 퍼진다.

## 재는 것

전체 소요만으로는 스큐를 설명할 수 없다. 느려진 이유가 일이 많아서인지 한
태스크에 몰려서인지 구분되지 않는다. 이벤트 로그에서 태스크별 시간을 꺼내
**최장/중앙값 비율**을 함께 본다(flux.common.eventlog).

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \\
        python scripts/experiment_skew.py
"""

import argparse
import json
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from flux.common import config, eventlog
from flux.common.spark import session as spark_session
from flux.gold import fact, mart, mart_build, skew


def event_dir(root: Path, run: str) -> Path:
    path = root / "eventlogs" / run
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_once(cfg, rep: int, *, fact_path: str, root: Path,
             shuffle_partitions: int) -> dict:
    """구성 하나를 한 번 돌리고 수치를 거둔다.

    **세션을 매번 새로 만들고 끝나면 닫는다.** 한 세션을 재사용하면 앞 실행이
    남긴 캐시와 통계가 다음 실행에 섞인다.
    """
    name = f"{cfg.name}-{rep}"
    logs = event_dir(root, name)

    extra = dict(cfg.spark_conf())
    extra["spark.eventLog.enabled"] = "true"
    extra["spark.eventLog.dir"] = logs.as_uri()

    spark = spark_session(f"flux-skew-{name}",
                          shuffle_partitions=shuffle_partitions, extra=extra)
    started = time.time()
    try:
        built = mart_build.build(spark, fact_path, salting=cfg.salting)
        # 집계를 실제로 시키려면 결과를 거둬야 한다. count()는 마지막 단계를
        # 한 줄로 접어 버리므로, 행을 다 만들게 하려고 합계를 함께 낸다.
        totals = built.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum("listens").alias("listens"),
        ).first()
        elapsed = time.time() - started
    finally:
        spark.stop()

    # 이벤트 로그는 세션을 닫아야 마저 쓰인다.
    #
    # **없으면 멈춘다.** 처음엔 못 찾으면 빈 결과로 넘어가게 뒀는데, 그러면
    # 최장/중앙 비율이 0.00으로 나오면서 실험은 멀쩡히 끝난 것처럼 보였다.
    # 스큐가 없다는 측정값과 구분이 안 된다.
    summary = eventlog.summary(eventlog.read(logs))
    if not summary["stages"]:
        raise eventlog.NoEventLog(f"단계가 하나도 안 잡혔다: {logs}")

    return {
        "config": cfg.name,
        "repeat": rep,
        "wall_seconds": round(elapsed, 1),
        "mart_rows": totals["rows"],
        "listens": totals["listens"],
        **summary,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--repeats", type=int, default=skew.REPEATS)
    parser.add_argument("--only", default=None,
                        help="구성 하나만 돌린다. 디버깅용")
    args = parser.parse_args()

    fact_path = config.gold(fact.TABLE_NAME)
    root = config.local_root()
    configs = [skew.config(args.only)] if args.only else list(skew.CONFIGS)

    print("구성")
    for c in configs:
        print(f"  {c.name:10} {c.label}")
    print(f"회차 {args.repeats}회, 총 {len(configs) * args.repeats}런")
    print()

    results = []
    # 회차를 바깥에 둔다. 구성을 바깥에 두면 캐시가 뒤 구성에 유리해진다.
    for rep in range(1, args.repeats + 1):
        for cfg in configs:
            row = run_once(cfg, rep, fact_path=fact_path, root=root,
                           shuffle_partitions=args.shuffle_partitions)
            results.append(row)
            top = row.get("heaviest") or {}
            print(f"  {rep}회차 {cfg.name:10} "
                  f"{row['wall_seconds']:7.1f}초  "
                  f"최장/중앙 {top.get('max_over_median', 0):6.2f}  "
                  f"셔플 w {row['shuffle_write_bytes'] / 1e9:6.2f}GB")

    print()
    print(f"{'구성':12} {'중앙 소요':>9} {'최장/중앙':>9} "
          f"{'셔플 read':>10} {'셔플 write':>11} {'마트 행':>10}")
    table = []
    for cfg in configs:
        rows = [r for r in results if r["config"] == cfg.name]
        if not rows:
            continue
        walls = sorted(r["wall_seconds"] for r in rows)
        ratios = [(r.get("heaviest") or {}).get("max_over_median", 0)
                  for r in rows]
        entry = {
            "config": cfg.name,
            "label": cfg.label,
            "runs": len(rows),
            "wall_median": walls[len(walls) // 2],
            "wall_min": walls[0],
            "wall_max": walls[-1],
            "max_over_median": round(sum(ratios) / len(ratios), 2),
            "shuffle_read_bytes": rows[0]["shuffle_read_bytes"],
            "shuffle_write_bytes": rows[0]["shuffle_write_bytes"],
            "mart_rows": rows[0]["mart_rows"],
        }
        table.append(entry)
        print(f"{cfg.name:12} {entry['wall_median']:8.1f}초 "
              f"{entry['max_over_median']:9.2f} "
              f"{entry['shuffle_read_bytes'] / 1e9:9.2f}GB "
              f"{entry['shuffle_write_bytes'] / 1e9:10.2f}GB "
              f"{entry['mart_rows']:>10,}")

    # 마트 행 수가 구성마다 다르면 같은 것을 잰 것이 아니다.
    distinct_rows = {e["mart_rows"] for e in table}
    print()
    if len(distinct_rows) == 1:
        print(f"대조 통과 : 네 구성이 같은 결과를 냈다 ({distinct_rows.pop():,}행)")
    else:
        print(f"대조 실패 : 구성마다 마트 행 수가 다르다 {sorted(distinct_rows)}")

    config.write_measurement("skew-experiment", {
        "shuffle_partitions": args.shuffle_partitions,
        "repeats": args.repeats,
        "runs": results,
        "summary": table,
    })
    return 0 if len(distinct_rows) == 1 else 1


if __name__ == "__main__":
    sys.exit(main())
