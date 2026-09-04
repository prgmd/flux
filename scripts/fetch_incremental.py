"""증분 덤프 1일분을 받아 체크섬까지 검증한다.

    FLUX_STORAGE_ROOT=... python scripts/fetch_incremental.py [--keyword listens-dump]
"""

import argparse
import sys
import urllib.request
from pathlib import Path
from urllib.parse import urlsplit

from flux.common import config
from flux.ingest import catalog, checksum, download
from flux.ingest.catalog import DumpKind


def local_dir() -> Path:
    """저장 루트가 로컬일 때만 실제 경로로 바꾼다."""
    root = config.storage_root()
    if not config.is_local():
        raise SystemExit(f"로컬 경로가 아니다: {root}. 지금은 file:// 만 지원한다.")
    return Path(urlsplit(root).path.lstrip("/"))


def fetch_text(url: str) -> str:
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read().decode("utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keyword", default="listens-dump",
                        help="받을 파일 이름에 들어갈 키워드")
    args = parser.parse_args()

    dump = catalog.latest_dump(DumpKind.INCREMENTAL)
    print(f"최신 증분 덤프 : {dump.name}  ({dump.dumped_on})")

    target = catalog.find_file(catalog.list_files(dump), args.keyword)
    print(f"받을 파일      : {target.name}")

    dest_dir = local_dir() / "dumps" / "listenbrainz" / "incremental" / dump.name
    dest = dest_dir / target.name
    size = download.remote_size(target.url)
    print(f"저장 위치      : {dest}")
    print(f"크기           : {size:,} 바이트" if size else "크기: 알 수 없음")

    if dest.exists():
        print("이미 받아 둔 파일이 있다. 다시 받지 않고 검증만 한다.")
    else:
        last = [0]

        def report(received, total):
            if received - last[0] >= 20 * 1024 * 1024 or received == total:
                print(f"  {download.format_progress(received, total)}", flush=True)
                last[0] = received

        download.download(target.url, dest, progress=report)

    expectations = []
    for algorithm, url in (("md5", target.md5_url), ("sha256", target.sha256_url)):
        if url:
            expectations.append((algorithm, fetch_text(url)))

    print("체크섬 검증 중...")
    results = checksum.verify_all(dest, expectations)
    for algorithm, digest in results.items():
        print(f"  {algorithm:6} {digest}  일치")

    print(f"\n완료: {dest.stat().st_size:,} 바이트")
    return 0


if __name__ == "__main__":
    sys.exit(main())
