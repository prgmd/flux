"""MusicBrainz 코어 덤프를 풀지 않고 테이블 행 수를 센다.

    FLUX_STORAGE_ROOT=... python scripts/inspect_musicbrainz.py
    FLUX_STORAGE_ROOT=... python scripts/inspect_musicbrainz.py --all
"""

import argparse
import json
import sys
import time
from pathlib import Path

from flux.common import config
from flux.ingest import mbdump_reader as reader

# S1 게이트가 요구하는 것은 artist 행 수다.
# 나머지는 S3의 차원 테이블 설계에 쓸 규모 감각을 얻기 위해 함께 센다.
DEFAULT_TABLES = ["artist", "recording", "release", "release_group", "artist_credit"]


# 윈도우 콘솔 기본 인코딩(cp949)은 MusicBrainz의 다국어 이름을 출력하지 못한다.
# 집계는 문제없는데 화면에 찍다가 죽는 것을 막는다.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")


def find_dump(root: Path) -> Path:
    base = root / "dumps" / "musicbrainz"
    candidates = sorted(base.glob("*/mbdump.tar.bz2"))
    if not candidates:
        raise SystemExit(f"코어 덤프를 찾지 못했다: {base}")
    return candidates[-1]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="모든 테이블을 센다 (오래 걸린다)")
    parser.add_argument("--sample", type=int, default=1, help="테이블마다 앞의 N줄을 보여준다")
    args = parser.parse_args()

    dump = find_dump(config.local_root())
    print(f"덤프: {dump}")
    print(f"압축 크기: {dump.stat().st_size:,} 바이트\n")

    wanted = None if args.all else DEFAULT_TABLES
    if wanted:
        print(f"대상 테이블: {', '.join(wanted)}")
        print("(찾는 것을 다 세면 끝까지 읽지 않고 멈춘다)\n")

    started = time.time()
    results = []
    for stats in reader.read_tables(dump, wanted, sample_rows=args.sample):
        elapsed = time.time() - started
        print(f"{stats.name:22} {stats.rows:>12,}행  "
              f"{stats.bytes_read / 1024**2:>9,.1f}MB  "
              f"평균 {stats.avg_row_bytes:>6.1f}B  ({elapsed:.0f}초)", flush=True)
        if stats.sample:
            preview = stats.sample[0]
            print(f"{'':22} └ {preview[:110]}")
        results.append({
            "table": stats.name,
            "rows": stats.rows,
            "bytes": stats.bytes_read,
            "avg_row_bytes": stats.avg_row_bytes,
        })

    if not results:
        print("센 테이블이 없다.")
        return 1

    payload = {
        "dump": dump.name,
        "release": dump.parent.name,
        "compressed_bytes": dump.stat().st_size,
        "elapsed_seconds": round(time.time() - started, 1),
        "tables": results,
    }
    out = config.local_root() / "measurements"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"musicbrainz-{dump.parent.name}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n총 {len(results)}개 테이블, {payload['elapsed_seconds']}초")
    print(f"기록: {path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
