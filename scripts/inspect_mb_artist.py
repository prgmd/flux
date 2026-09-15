"""MusicBrainz artist 테이블의 실제 모양을 조사한다.

덤프의 테이블 파일에는 **헤더가 없다.** PostgreSQL COPY 형식이라
컬럼 순서는 MusicBrainz 스키마 정의에서 와야 하고, 파일만 봐서는 알 수 없다.

그래서 두 가지를 확인한다.

1. 컬럼 개수가 일정한가. 일정해야 위치로 읽을 수 있다
2. 각 자리의 값이 **우리가 아는 컬럼 순서**와 맞는가

2번이 핵심이다. 순서를 틀리면 이름 자리에 나라 코드가 들어가는데,
그래도 프로그램은 돌고 결과만 조용히 틀린다.

    FLUX_STORAGE_ROOT=... python scripts/inspect_mb_artist.py --rows 20
"""

import argparse
import sys
from collections import Counter

from flux.common import config
from flux.ingest import mbdump_reader

TABLE = "artist"
NULL = "\\N"

# MusicBrainz 스키마의 artist 테이블 컬럼 순서.
# 출처는 파일이 아니라 스키마 정의다. 실제 값과 맞는지 아래에서 확인한다.
EXPECTED_COLUMNS = (
    "id", "gid", "name", "sort_name",
    "begin_date_year", "begin_date_month", "begin_date_day",
    "end_date_year", "end_date_month", "end_date_day",
    "type", "area", "gender", "comment",
    "edits_pending", "last_updated", "ended",
    "begin_area", "end_area",
)

UUID_LENGTH = 36


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=20, help="엿볼 행 수")
    args = parser.parse_args()

    root = config.local_root()
    dumps = sorted((root / "dumps" / "musicbrainz").glob("*/mbdump.tar.bz2"))
    if not dumps:
        raise SystemExit("mbdump.tar.bz2를 찾지 못했다")
    dump = dumps[-1]
    print(f"덤프 : {dump.parent.name}")
    print(f"표본 : {args.rows}행")
    print()

    stats = next(mbdump_reader.read_tables(
        dump, wanted={TABLE}, sample_rows=args.rows))

    widths = Counter()
    nulls = Counter()
    for line in stats.sample:
        fields = line.split("\t")
        widths[len(fields)] += 1
        for i, value in enumerate(fields):
            if value == NULL:
                nulls[i] += 1

    print(f"행 수     : {stats.rows:,}")
    print(f"컬럼 개수 : {dict(widths)}")
    print(f"기대 개수 : {len(EXPECTED_COLUMNS)}")
    print()

    if len(widths) != 1:
        print("컬럼 개수가 일정하지 않다. 위치로 읽을 수 없다.")
        return 1

    found = next(iter(widths))
    if found != len(EXPECTED_COLUMNS):
        print(f"컬럼 개수가 다르다. 실제 {found}, 기대 {len(EXPECTED_COLUMNS)}")
        print("스키마 정의를 다시 확인해야 한다.")

    print("자리별 값 (첫 3행)")
    for line in stats.sample[:3]:
        fields = line.split("\t")
        print()
        for i, value in enumerate(fields):
            name = EXPECTED_COLUMNS[i] if i < len(EXPECTED_COLUMNS) else f"({i})"
            shown = value if len(value) <= 40 else value[:37] + "..."
            print(f"  {i:>2} {name:20} {shown}")

    # 순서가 맞는지 값의 모양으로 검산한다.
    print()
    print("순서 검산")
    checks = []
    for line in stats.sample:
        fields = line.split("\t")
        if len(fields) != len(EXPECTED_COLUMNS):
            continue
        row = dict(zip(EXPECTED_COLUMNS, fields))
        checks.append({
            "id가 숫자": row["id"].isdigit(),
            "gid가 UUID 길이": len(row["gid"]) == UUID_LENGTH and "-" in row["gid"],
            "name이 비지 않음": bool(row["name"].strip()),
            "ended가 t 또는 f": row["ended"] in ("t", "f"),
            "edits_pending이 숫자": row["edits_pending"].isdigit(),
        })

    for label in checks[0] if checks else []:
        passed = sum(1 for c in checks if c[label])
        mark = "통과" if passed == len(checks) else f"{passed}/{len(checks)}"
        print(f"  {label:22} {mark}")

    print()
    print(f"비어 있는 자리 (표본 {len(stats.sample)}행 중)")
    for i in sorted(nulls):
        name = EXPECTED_COLUMNS[i] if i < len(EXPECTED_COLUMNS) else f"({i})"
        print(f"  {i:>2} {name:20} {nulls[i]:>4}회")

    payload = {
        "dump": dump.parent.name,
        "table": TABLE,
        "rows": stats.rows,
        "column_widths": dict(widths),
        "expected_columns": list(EXPECTED_COLUMNS),
        "null_counts_by_index": {str(k): v for k, v in nulls.items()},
        "sample": stats.sample[:5],
    }
    config.write_measurement("mb-artist-shape", payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
