"""MusicBrainz 코어 덤프를 받아 체크섬까지 검증한다.

    FLUX_STORAGE_ROOT=... python scripts/pipeline/fetch_musicbrainz.py
"""

import argparse
import sys

from flux.common import config
from flux.ingest import checksum, download, manifest as manifest_mod
from flux.ingest import musicbrainz as mb


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--release", default=None,
                        help="받을 릴리스를 직접 지정한다. 없으면 진행 중인 것이나 LATEST를 쓴다.")
    args = parser.parse_args()

    root = config.local_root()
    mb_dir = root / "dumps" / "musicbrainz"
    release, warning = mb.resolve_release(mb_dir, args.release)
    if warning:
        print(f"주의: {warning}")
    print(f"대상 릴리스: {release}")

    dest_dir = mb_dir / release
    book = manifest_mod.Manifest(root / "dumps" / manifest_mod.MANIFEST_NAME)

    for target in mb.list_files(release):
        dest = dest_dir / target.name
        print(f"\n── {target.name}")

        if book.is_complete(release, target.name):
            print("  이미 받아 둔 기록이 있다. 건너뛴다.")
            continue

        if not dest.exists():
            size = download.remote_size(target.url)
            print(f"  크기: {size:,} 바이트" if size else "  크기: 알 수 없음")
            last = [0]

            def report(received, total, _last=last):
                if received - _last[0] >= 200 * 1024 * 1024 or received == total:
                    print(f"    {download.format_progress(received, total)}", flush=True)
                    _last[0] = received

            download.download_with_retry(target.url, dest, progress=report)
        else:
            print("  파일은 있으나 기록이 없다. 검증만 한다.")

        print("  체크섬 검증 중...")
        results = checksum.verify_all(dest, target.checksums)
        for algorithm, digest in results.items():
            print(f"    {algorithm:6} {digest}  일치")

        book.record(release, target.name, dest, results)
        print(f"  기록 완료 ({dest.stat().st_size:,} 바이트)")

    print(f"\n매니페스트: {book.path}  (총 {len(book)}건)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
