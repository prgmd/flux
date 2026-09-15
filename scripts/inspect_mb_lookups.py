"""artist가 참조하는 조회 테이블들을 조사한다.

artist의 type, area, gender는 **숫자 외래 키**다. 명세 5.3의 dim_artist는
country와 type을 값으로 갖는데, 그러려면 이 표들을 함께 실어 풀어야 한다.

덤프 파일에 헤더가 없으므로 여기서도 컬럼 순서를 값의 모양으로 검산한다.

    FLUX_STORAGE_ROOT=... python scripts/inspect_mb_lookups.py
"""

import argparse
import sys
from collections import Counter

from flux.common import config
from flux.ingest import mbdump_reader

NULL = "\\N"

# 스키마 정의에서 온 컬럼 순서. 파일에는 없다.
EXPECTED = {
    "area": ("id", "gid", "name", "type", "edits_pending", "last_updated",
             "begin_date_year", "begin_date_month", "begin_date_day",
             "end_date_year", "end_date_month", "end_date_day", "ended",
             "comment"),
    "artist_type": ("id", "name", "parent", "child_order", "description", "gid"),
    "gender": ("id", "name", "parent", "child_order", "description", "gid"),
    "iso_3166_1": ("area", "code"),
}

SHOWN_FIELDS = ("id", "gid", "name", "code", "area", "type")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=40)
    args = parser.parse_args()

    root = config.local_root()
    dumps = sorted((root / "dumps" / "musicbrainz").glob("*/mbdump.tar.bz2"))
    if not dumps:
        raise SystemExit("mbdump.tar.bz2를 찾지 못했다")
    dump = dumps[-1]
    print(f"덤프 : {dump.parent.name}")
    print()

    payload = {"dump": dump.parent.name, "tables": {}}

    for stats in mbdump_reader.read_tables(
            dump, wanted=set(EXPECTED), sample_rows=args.rows):
        expected = EXPECTED[stats.name]
        widths = Counter(len(line.split("\t")) for line in stats.sample)

        print(f"=== {stats.name}")
        print(f"  행 수     : {stats.rows:,}")
        print(f"  컬럼 개수 : {dict(widths)}  (기대 {len(expected)})")

        if len(widths) == 1 and next(iter(widths)) == len(expected):
            print("  컬럼 개수 일치")
        else:
            print("  컬럼 개수가 다르다. 스키마 정의를 확인해야 한다")

        print("  표본")
        for line in stats.sample[:6]:
            fields = line.split("\t")
            shown = ", ".join(
                f"{name}={value if value != NULL else 'NULL'}"
                for name, value in zip(expected, fields)
                if name in SHOWN_FIELDS
            )
            print(f"    {shown}")
        print()

        payload["tables"][stats.name] = {
            "rows": stats.rows,
            "column_widths": {str(k): v for k, v in widths.items()},
            "expected_columns": list(expected),
            "sample": stats.sample[:5],
        }

    config.write_measurement("mb-lookup-shape", payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
