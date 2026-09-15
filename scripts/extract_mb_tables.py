"""mbdump.tar.bz2에서 dim_artist에 필요한 표만 꺼내 스테이징에 둔다.

Spark는 tar.bz2를 직접 읽지 못한다. 청취 덤프 때와 같은 이유로 한 번은 풀어야 하고,
필요한 표만 꺼낸다. 다섯 표를 합쳐도 450MB 남짓이라 7GB 전체를 풀 이유가 없다.

꺼낸 파일에는 헤더를 붙이지 않는다. 원본에 없던 줄을 넣으면 그 파일이
더 이상 원본과 같지 않고, 줄 수 대조가 어긋난다. 컬럼 이름은 적재 쪽이
flux.silver.musicbrainz의 선언에서 가져온다.

    FLUX_STORAGE_ROOT=... python scripts/extract_mb_tables.py
"""

import argparse
import sys
import tarfile
import time

from flux.common import config
from flux.ingest import mbdump_reader
from flux.silver import musicbrainz as mb

CHUNK = 8 * 1024 * 1024
STAGING = "staging/musicbrainz"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", default=None, help="없으면 가장 최근 것")
    args = parser.parse_args()

    root = config.local_root()
    base = root / "dumps" / "musicbrainz"
    dumps = sorted(base.glob("*/mbdump.tar.bz2"))
    if args.release:
        dumps = [d for d in dumps if d.parent.name == args.release]
    if not dumps:
        raise SystemExit("mbdump.tar.bz2를 찾지 못했다")

    dump = dumps[-1]
    release = dump.parent.name
    out_dir = root / STAGING / release
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"릴리스   : {release}")
    print(f"스테이징 : {out_dir}")
    print(f"꺼낼 표  : {', '.join(mb.REQUIRED_TABLES)}")
    print()

    remaining = set(mb.REQUIRED_TABLES)
    written = {}
    started = time.time()

    with tarfile.open(dump, mode="r|bz2") as tar:
        for member in tar:
            if not member.isfile():
                continue
            name = mbdump_reader.table_name(member.name)
            if name not in remaining:
                continue

            target = out_dir / name
            part = target.with_name(name + ".part")
            stream = tar.extractfile(member)
            done = 0
            with open(part, "wb") as fp:
                while chunk := stream.read(CHUNK):
                    fp.write(chunk)
                    done += len(chunk)
            part.replace(target)

            written[name] = done
            remaining.discard(name)
            print(f"  {name:14} {done / 1024**2:8.1f}MB  "
                  f"({time.time() - started:.0f}초)")
            if not remaining:
                break

    if remaining:
        print()
        print(f"못 찾은 표: {', '.join(sorted(remaining))}")
        return 1

    print()
    print(f"합계 {sum(written.values()) / 1024**2:.1f}MB, "
          f"{time.time() - started:.0f}초")

    payload = {
        "release": release,
        "elapsed_seconds": round(time.time() - started, 1),
        "tables": written,
    }
    config.write_measurement("mb-extract", payload)
    return 0


if __name__ == "__main__":
    sys.exit(main())
