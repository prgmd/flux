"""증분 덤프를 최근 N일치까지 이어서 받는다.

1일분을 받는 fetch_incremental.py와 같은 부품을 쓰되 여러 덤프를 돈다.
매니페스트가 이미 받은 것을 건너뛰므로 중간에 끊고 다시 돌려도 된다.

    FLUX_STORAGE_ROOT=... python scripts/pipeline/fetch_incremental_range.py --days 30
"""

import argparse
import sys
import urllib.request
from pathlib import Path

from flux.common import config
from flux.ingest import catalog, checksum, download, manifest as manifest_mod
from flux.ingest.catalog import DumpKind

KEYWORD = "listens-dump"


def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read().decode("utf-8", errors="replace")


def fetch_one(dump, root: Path, book) -> tuple[str, int]:
    """덤프 하나를 받아 검증하고 기록한다. (상태, 바이트) 를 돌려준다."""
    target = catalog.find_file(catalog.list_files(dump), KEYWORD)
    dest = root / "dumps" / "listenbrainz" / "incremental" / dump.name / target.name

    if book.is_complete(dump.name, target.name):
        return "건너뜀", dest.stat().st_size

    last = [0]

    def report(received, total):
        if received - last[0] >= 50 * 1024 * 1024 or received == total:
            print(f"    {download.format_progress(received, total)}", flush=True)
            last[0] = received

    if not dest.exists():
        download.download_with_retry(target.url, dest, progress=report)

    expectations = []
    for algorithm, url in (("md5", target.md5_url), ("sha256", target.sha256_url)):
        if url:
            expectations.append((algorithm, fetch_text(url)))
    results = checksum.verify_all(dest, expectations)
    book.record(dump.name, target.name, dest, results)
    return "받음", dest.stat().st_size


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30, help="최근 며칠치까지 받을지")
    args = parser.parse_args()

    root = config.local_root()
    available = catalog.list_dumps(DumpKind.INCREMENTAL)
    wanted = catalog.recent_dumps(available, args.days)

    print(f"서버 보유 : {len(available)}개 "
          f"({available[0].dumped_on} ~ {available[-1].dumped_on})")
    print(f"받을 대상 : {len(wanted)}개 (최근 {args.days}일)\n")

    book = manifest_mod.Manifest(root / "dumps" / manifest_mod.MANIFEST_NAME)
    total = 0
    failed = []
    for i, dump in enumerate(wanted, 1):
        print(f"[{i}/{len(wanted)}] {dump.dumped_on}  {dump.name}", flush=True)
        try:
            state, size = fetch_one(dump, root, book)
        except Exception as error:                    # noqa: BLE001
            print(f"  실패: {type(error).__name__}: {error}", flush=True)
            failed.append(dump.name)
            continue
        total += size
        print(f"  {state}  {size / 1024**2:.0f}MB", flush=True)

    print(f"\n합계 {total / 1024**3:.2f}GB")
    if failed:
        print(f"실패 {len(failed)}개: {', '.join(failed)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
