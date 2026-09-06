"""증분 덤프에서 .listens(jsonl)만 꺼내 적재용 스테이징에 둔다.

Spark는 tar.zst를 직접 읽지 못한다. 그래서 한 번은 풀어야 하는데,
필요한 것은 안에 든 .listens 파일 하나뿐이므로 그것만 꺼낸다.

스테이징은 데이터 루트 아래 staging/에 두고, 적재가 끝나면 지워도 된다.
원본 tar.zst가 남아 있으므로 언제든 다시 만들 수 있다.
"""

import argparse
import sys
import tarfile
from compression import zstd
from pathlib import Path
from urllib.parse import urlsplit

from flux.common import config

CHUNK = 8 * 1024 * 1024


def local_root() -> Path:
    root = config.storage_root()
    if not config.is_local():
        raise SystemExit(f"로컬 경로가 아니다: {root}")
    return Path(urlsplit(root).path.lstrip("/"))


def find_dumps(root: Path) -> list[Path]:
    base = root / "dumps" / "listenbrainz" / "incremental"
    return sorted(base.glob("*/listenbrainz-listens-dump-*.tar.zst"))


def extract(dump: Path, out_dir: Path) -> list[Path]:
    """덤프 안의 .listens를 꺼낸다. 이미 있으면 건너뛴다."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written = []

    with zstd.ZstdFile(dump) as z, tarfile.open(fileobj=z, mode="r|") as tar:
        for member in tar:
            if not member.name.endswith(".listens"):
                continue
            # listens/2026/9.listens → 2026-9.listens
            flat = "-".join(Path(member.name).parts[-2:])
            target = out_dir / flat

            if target.exists() and target.stat().st_size == member.size:
                print(f"  이미 있음: {target.name} ({member.size:,} 바이트)")
                written.append(target)
                continue

            part = target.with_name(target.name + ".part")
            stream = tar.extractfile(member)
            done = 0
            with open(part, "wb") as fp:
                while chunk := stream.read(CHUNK):
                    fp.write(chunk)
                    done += len(chunk)
                    if done % (500 * 1024 * 1024) < CHUNK:
                        print(f"    {done / 1024**3:.2f}GB / {member.size / 1024**3:.2f}GB",
                              flush=True)
            part.replace(target)
            print(f"  꺼냄: {target.name} ({done:,} 바이트)")
            written.append(target)

    return written


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", default=None, help="덤프 디렉터리 이름. 없으면 최신")
    args = parser.parse_args()

    root = local_root()
    dumps = find_dumps(root)
    if not dumps:
        raise SystemExit("증분 덤프를 찾지 못했다")

    if args.dump:
        dumps = [d for d in dumps if d.parent.name == args.dump]
        if not dumps:
            raise SystemExit(f"덤프를 찾지 못했다: {args.dump}")

    dump = dumps[-1]
    out_dir = root / "staging" / dump.parent.name
    print(f"덤프    : {dump.parent.name}")
    print(f"스테이징: {out_dir}\n")

    files = extract(dump, out_dir)
    total = sum(f.stat().st_size for f in files)
    print(f"\n{len(files)}개 파일, {total / 1024**3:.2f}GB")
    return 0


if __name__ == "__main__":
    sys.exit(main())
