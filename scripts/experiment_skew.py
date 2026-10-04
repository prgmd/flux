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
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from flux.common import config, eventlog
from flux.common.spark import master as spark_master
from flux.common.spark import session as spark_session
from flux.gold import fact, mart_build, scd2, skew


def event_dir(root: Path, out: str, run: str) -> Path:
    """실행 하나의 이벤트 로그 자리. **실측 파일 이름 아래에 둔다.**

    처음엔 `eventlogs/<구성>-<회차>`였다. 그러면 다른 실험이 같은 구성 이름으로
    돌 때 앞 실험의 원자료를 지운다. 9월 18일 groupBy 실험의 로그가 9월 19일 조인
    실험에 그렇게 덮였고, 나중에 지표를 고쳤을 때 그 실험만 다시 뽑을 수 없었다.
    """
    path = root / "eventlogs" / out / run
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def run_once(cfg, rep: int, *, fact_path: str, dim_path: str, root: Path,
             shuffle_partitions: int, out: str) -> dict:
    """구성 하나를 한 번 돌리고 수치를 거둔다.

    **세션을 매번 새로 만들고 끝나면 닫는다.** 한 세션을 재사용하면 앞 실행이
    남긴 캐시와 통계가 다음 실행에 섞인다.
    """
    name = f"{cfg.name}-{rep}"
    logs = event_dir(root, out, name)

    extra = dict(cfg.spark_conf())
    extra["spark.eventLog.enabled"] = "true"
    extra["spark.eventLog.dir"] = logs.as_uri()

    spark = spark_session(f"flux-skew-{name}",
                          shuffle_partitions=shuffle_partitions, extra=extra)
    started = time.time()
    try:
        # **재는 것은 조인이다.** groupBy는 셔플 전에 부분 집계로 접혀
        # 스큐가 셔플에 도달하지 않는다
        # (docs/measurements/2026-09-19-physical-plans.md).
        dim = spark.read.format("delta").load(dim_path)
        built = mart_build.join_for_experiment(
            spark, fact_path, dim, salting=cfg.salting)
        # 조인을 실제로 시키려면 결과를 거둬야 한다. 행 수와 이름이 붙은 수를
        # 함께 세어 구성끼리 같은 것을 냈는지 대조한다.
        totals = built.agg(
            F.count(F.lit(1)).alias("rows"),
            F.sum(F.col("name").isNotNull().cast("long")).alias("listens"),
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
    parser.add_argument("--out", default="skew-experiment",
                        help="실측 파일 이름. 로컬과 클러스터 결과가 "
                             "서로 덮어쓰지 않게 나눈다")
    args = parser.parse_args()

    fact_path = config.gold(fact.TABLE_NAME)
    dim_path = config.gold(scd2.TABLE_NAME)
    root = config.local_root()
    master = spark_master()
    configs = [skew.config(args.only)] if args.only else list(skew.CONFIGS)

    print("구성")
    for c in configs:
        print(f"  {c.name:10} {c.label}")
    print(f"회차 {args.repeats}회, 총 {len(configs) * args.repeats}런")
    print(f"마스터 {master}, 셔플 파티션 {args.shuffle_partitions}")
    print(f"실측 파일 {args.out}, 이벤트 로그 eventlogs/{args.out}/")
    print()

    results = []
    # 회차를 바깥에 둔다. 구성을 바깥에 두면 캐시가 뒤 구성에 유리해진다.
    for rep in range(1, args.repeats + 1):
        for cfg in configs:
            row = run_once(cfg, rep, fact_path=fact_path, dim_path=dim_path,
                           root=root,
                           shuffle_partitions=args.shuffle_partitions,
                           out=args.out)
            results.append(row)
            skewed = row.get("join_stage") or {}
            print(f"  {rep}회차 {cfg.name:10} "
                  f"{row['wall_seconds']:7.1f}초  "
                  f"최장/중앙 {skewed.get('max_over_median', 0):6.2f}  "
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
        ratios = [(r.get("join_stage") or {}).get("max_over_median", 0)
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
            "listens": rows[0]["listens"],
        }
        table.append(entry)
        print(f"{cfg.name:12} {entry['wall_median']:8.1f}초 "
              f"{entry['max_over_median']:9.2f} "
              f"{entry['shuffle_read_bytes'] / 1e9:9.2f}GB "
              f"{entry['shuffle_write_bytes'] / 1e9:10.2f}GB "
              f"{entry['mart_rows']:>10,}")

    # 마트 행 수가 구성마다 다르면 같은 것을 잰 것이 아니다.
    # set.pop()은 꺼내면서 **지운다.** 출력문 안에서 부르는 바람에 집합이
    # 비워져, 대조는 통과했는데 종료코드가 1로 나갔다. 출력에 부작용을
    # 섞지 않는다.
    distinct_rows = sorted({e["mart_rows"] for e in table})
    distinct_named = sorted({e["listens"] for e in table})
    agreed = len(distinct_rows) == 1 and len(distinct_named) == 1
    print()
    if agreed:
        print(f"대조 통과 : {len(table)}개 구성이 같은 결과를 냈다 "
              f"({distinct_rows[0]:,}행, 이름 붙음 {distinct_named[0]:,})")
    else:
        print(f"대조 실패 : 행 수 {distinct_rows}, 이름 붙은 수 {distinct_named}")

    config.write_measurement(args.out, {
        "master": master,
        "shuffle_partitions": args.shuffle_partitions,
        "repeats": args.repeats,
        "runs": results,
        "summary": table,
    })
    return 0 if agreed else 1


if __name__ == "__main__":
    sys.exit(main())
