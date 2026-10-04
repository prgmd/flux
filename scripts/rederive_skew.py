"""저장된 이벤트 로그에서 스큐 수치를 다시 뽑는다.

실험을 다시 돌릴 필요가 없다. **원자료가 남아 있기 때문이다.**

`experiment_skew.py`는 실행마다 `eventlogs/<실측 이름>/<구성>-<회차>/`에 로그를 따로
남긴다. 그러니 지표를 어떻게 세는지가 틀렸어도 **로그를 다시 읽으면 된다.**
12런에 40분이 드는데 다시 읽는 것은 몇 초다.

## 왜 다시 뽑는가

처음엔 스큐 지표로 `heaviest`(총 태스크 시간이 가장 큰 단계)를 썼다.
조인 잡에서 그 단계는 **입력 파일을 읽는 스캔**이고, 파일 단위로 나뉘어
원래 고르다. 최장/중앙이 1.09로 나왔다.

스큐는 **셔플 뒤 단계**에 있었다. 태스크 수가 셔플 파티션 수와 같고
최장/중앙이 4.94였다. 총 시간은 스캔의 20분의 1이라 `heaviest`에 밀렸다.

**큰 단계와 치우친 단계가 다르다.**

## 두 번째로 다시 뽑는 이유

그래서 "가장 치우친 단계"로 바꿨는데, 이번엔 Delta 트랜잭션 로그를 읽는 장부
단계가 이겼다. 50 태스크에 셔플 21.7MB뿐이라 태스크 하나만 늦어도 비율이 튄다.
로컬 aqe_on 세 런과 both 한 런이 그 단계의 값이었다. 지금은 **셔플을 가장 많이
읽는 단계**를 본다(flux.common.eventlog.join_stage).

    PYTHONPATH=src venv/Scripts/python scripts/rederive_skew.py \
        --logs eventlogs-local --out skew-rederived-local
"""

import argparse
import sys

from flux.common import config, eventlog
from flux.gold import skew


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--logs", default="eventlogs",
                        help="데이터 루트 아래 이벤트 로그 디렉터리")
    parser.add_argument("--out", default="skew-rederived")
    args = parser.parse_args()

    root = config.local_root()
    logs = root / args.logs
    if not logs.is_dir():
        print(f"이벤트 로그가 없다: {logs}")
        return 1

    plans = root / config.MEASUREMENTS / "plans" / args.out
    plans.mkdir(parents=True, exist_ok=True)

    rows = []
    for cfg in skew.CONFIGS:
        for rep in range(1, skew.REPEATS + 1):
            path = logs / f"{cfg.name}-{rep}"
            if not path.is_dir():
                continue
            try:
                summary = eventlog.summary(eventlog.read(path))
            except eventlog.NoEventLog:
                print(f"{cfg.name}-{rep}: 로그 없음")
                continue
            plan = eventlog.join_plan(eventlog.lines(path)) or ""
            plan_file = plans / f"{cfg.name}-{rep}.txt"
            plan_file.write_text(plan, encoding="utf-8")
            joins = {op: plan.count(op) for op in eventlog.JOIN_OPERATORS
                     if op in plan}
            rows.append({"config": cfg.name, "repeat": rep, **summary,
                         "joins": joins,
                         "final_plan": "isFinalPlan=true" in plan,
                         "plan_file": str(plan_file.relative_to(root))})

    if not rows:
        print("읽을 수 있는 실행이 없다")
        return 1

    print(f"{'구성':11} {'회차':>3} {'조인단계':>8} {'태스크':>6} "
          f"{'시간비율':>8} {'행비율':>7} {'최장':>8} {'단계경과':>8} {'셔플write':>10}")
    for r in rows:
        s = r["join_stage"] or {}
        print(f"{r['config']:11} {r['repeat']:>3} "
              f"{s.get('stage_id', '-'):>8} {s.get('tasks', 0):>6,} "
              f"{s.get('max_over_median', 0):>8.2f} "
              f"{s.get('records_max_over_median', 0):>7.2f} "
              f"{s.get('max_ms', 0) / 1000:>7.2f}초 "
              f"{s.get('elapsed_ms', 0) / 1000:>7.1f}초 "
              f"{r['shuffle_write_bytes'] / 1e9:>9.2f}GB")

    def middle(values):
        values = sorted(values)
        return values[len(values) // 2]

    print()
    print(f"{'구성':11} {'시간비율':>16} {'행비율':>7} {'최장 태스크':>11} "
          f"{'조인 단계 경과':>13} {'런':>3}")
    table = []
    for cfg in skew.CONFIGS:
        mine = [r for r in rows if r["config"] == cfg.name]
        if not mine:
            continue
        joins = [r["join_stage"] or {} for r in mine]
        ratios = sorted(j.get("max_over_median", 0) for j in joins)
        entry = {
            "config": cfg.name,
            "label": cfg.label,
            "runs": len(mine),
            "skew_median": ratios[len(ratios) // 2],
            "skew_min": ratios[0],
            "skew_max": ratios[-1],
            "records_skew": middle(j.get("records_max_over_median", 0)
                                   for j in joins),
            "longest_task_ms": middle(j.get("max_ms", 0) for j in joins),
            "join_elapsed_ms": middle(j.get("elapsed_ms", 0) for j in joins),
            "join_tasks": middle(j.get("tasks", 0) for j in joins),
        }
        table.append(entry)
        print(f"{cfg.name:11} {entry['skew_median']:>5.2f} "
              f"({entry['skew_min']:.2f}~{entry['skew_max']:.2f}) "
              f"{entry['records_skew']:>7.2f} "
              f"{entry['longest_task_ms'] / 1000:>10.2f}초 "
              f"{entry['join_elapsed_ms'] / 1000:>12.1f}초 {entry['runs']:>3}")

    baseline = next((e for e in table if e["config"] == "aqe_off"), None)
    print()
    if baseline:
        value = baseline["skew_median"]
        print(f"기준선(aqe_off)의 치우침 중앙값 : {value:.2f}배")
        print(f"게이트(5배 이상)               : "
              f"{'통과' if value >= 5 else '미달'}")

    config.write_measurement(args.out, {"logs": args.logs, "runs": rows,
                                        "summary": table})
    return 0


if __name__ == "__main__":
    sys.exit(main())
