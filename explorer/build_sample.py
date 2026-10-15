"""탐색 화면이 쓰는 표본 파일을 만든다. 원본 Delta 표는 읽기만 한다.

    venv/Scripts/python explorer/build_sample.py [--dry-run]

사용자를 고르고(flux.explorer.pick) 그 사용자의 줄을 세 층 전부 `flux-data/explorer/sample.duckdb`에
담는다(flux.explorer.sample). 몇 분이 걸린다. --dry-run은 누구를 고르는지만 보여 주고 파일은 만들지 않는다.
"""

import argparse
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

import duckdb  # noqa: E402

from flux.common import config, queryrun  # noqa: E402
from flux.explorer import pick, sample  # noqa: E402


def data_root():
    if not os.environ.get(config.STORAGE_ROOT_ENV):
        env_file = REPO / ".env"
        value = queryrun.env_value(env_file.read_text(encoding="utf-8"), config.STORAGE_ROOT_ENV) \
            if env_file.exists() else None
        if not value:
            raise SystemExit(f"{config.STORAGE_ROOT_ENV}가 없다. 환경 변수나 .env에 데이터 루트를 적는다.")
        os.environ[config.STORAGE_ROOT_ENV] = value
    return config.local_root()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--seed", type=int, default=pick.DEFAULT_SEED)
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    root = data_root()
    base = root.as_posix()
    sources = {
        "bronze": queryrun.expand("{bronze}", base),
        "silver": queryrun.expand("{silver}", base),
        "fact": queryrun.expand("{fact}", base),
        "dim_artist": queryrun.expand("{dim_artist}", base),
        "quarantine": queryrun.expand("{silver_invalid}", base),
    }
    con = duckdb.connect()
    con.execute("LOAD delta")
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit='10GB'")
    con.execute("SET preserve_insertion_order=false")
    scratch = root / config.SCRATCH / "duckdb-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{scratch.as_posix()}'")

    started = time.time()
    stats = sample.read_user_stats(con, sources, pick.MIN_LISTENS, pick.MAX_LISTENS)
    chosen = pick.pick_users(stats, seed=args.seed)
    print(f"후보 {len(stats):,}명({pick.MIN_LISTENS:,}~{pick.MAX_LISTENS:,}건), 뽑은 사용자 {len(chosen)}명, "
          f"{time.time() - started:.0f}초")
    for label, quota, _ in pick.KINDS:
        got = sum(1 for kind in chosen.values() if kind == label)
        print(f"  {label:14} {got:>2}/{quota}")
    for label, missing in pick.shortfalls(chosen).items():
        print(f"  모자람: {label} {missing}명")
    if args.dry_run:
        return 0

    out = root / "explorer" / "sample.duckdb"
    counts = sample.extract(con, sources, chosen, out)
    print(f"표본 파일 {out} ({out.stat().st_size / 1024**2:.0f}MB), {time.time() - started:.0f}초")
    for name, rows in counts.items():
        print(f"  {name:14} {rows:>10,}행")
    return 0


if __name__ == "__main__":
    sys.exit(main())
