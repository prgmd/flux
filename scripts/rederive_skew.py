"""저장된 이벤트 로그에서 스큐 수치를 다시 뽑는다.

실험을 다시 돌릴 필요가 없다. **원자료가 남아 있기 때문이다.**

`experiment_skew.py`는 실행마다 `eventlogs/<구성>-<회차>/`에 로그를 따로
남긴다. 그러니 지표를 어떻게 세는지가 틀렸어도 **로그를 다시 읽으면 된다.**
12런에 40분이 드는데 다시 읽는 것은 몇 초다.

## 왜 다시 뽑는가

처음엔 스큐 지표로 `heaviest`(총 태스크 시간이 가장 큰 단계)를 썼다.
조인 잡에서 그 단계는 **입력 파일을 읽는 스캔**이고, 파일 단위로 나뉘어
원래 고르다. 최장/중앙이 1.09로 나왔다.

스큐는 **셔플 뒤 단계**에 있었다. 태스크 수가 셔플 파티션 수와 같고
최장/중앙이 4.94였다. 총 시간은 스캔의 20분의 1이라 `heaviest`에 밀렸다.

**큰 단계와 치우친 단계가 다르다.**

    venv/Scripts/python scripts/rederive_skew.py
"""

import sys
from pathlib import Path

from flux.common import config, eventlog
from flux.gold import skew


def main() -> int:
    root = config.local_root()
    logs = root / "eventlogs"
    if not logs.is_dir():
        print(f"이벤트 로그가 없다: {logs}")
        return 1

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
            rows.append({"config": cfg.name, "repeat": rep, **summary})

    if not rows:
        print("읽을 수 있는 실행이 없다")
        return 1

    print(f"{'구성':11} {'회차':>3} {'무거운단계':>10} {'그 비율':>8} "
          f"{'치우친단계':>10} {'태스크':>7} {'그 비율':>8} {'셔플write':>11}")
    for r in rows:
        h, s = r["heaviest"] or {}, r["most_skewed"] or {}
        print(f"{r['config']:11} {r['repeat']:>3} "
              f"{h.get('stage_id', '-'):>10} {h.get('max_over_median', 0):>8.2f} "
              f"{s.get('stage_id', '-'):>10} {s.get('tasks', 0):>7,} "
              f"{s.get('max_over_median', 0):>8.2f} "
              f"{r['shuffle_write_bytes'] / 1e9:>10.2f}GB")

    print()
    print(f"{'구성':11} {'치우침 중앙':>11} {'최소':>7} {'최대':>7} {'런':>3}")
    table = []
    for cfg in skew.CONFIGS:
        mine = [r for r in rows if r["config"] == cfg.name]
        if not mine:
            continue
        ratios = sorted((r["most_skewed"] or {}).get("max_over_median", 0)
                        for r in mine)
        entry = {
            "config": cfg.name,
            "label": cfg.label,
            "runs": len(mine),
            "skew_median": ratios[len(ratios) // 2],
            "skew_min": ratios[0],
            "skew_max": ratios[-1],
        }
        table.append(entry)
        print(f"{cfg.name:11} {entry['skew_median']:>11.2f} "
              f"{entry['skew_min']:>7.2f} {entry['skew_max']:>7.2f} "
              f"{entry['runs']:>3}")

    baseline = next((e for e in table if e["config"] == "aqe_off"), None)
    print()
    if baseline:
        value = baseline["skew_median"]
        print(f"기준선(aqe_off)의 치우침 중앙값 : {value:.2f}배")
        print(f"게이트(5배 이상)               : "
              f"{'통과' if value >= 5 else '미달'}")

    config.write_measurement("skew-rederived", {"runs": rows, "summary": table})
    return 0


if __name__ == "__main__":
    sys.exit(main())
