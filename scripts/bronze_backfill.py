"""받아 둔 증분 덤프를 날짜순으로 이어서 Bronze에 적재한다.

한 덤프마다 세 걸음을 돈다.

    꺼내기(호스트) → 적재(컨테이너) → 스테이징 삭제

스테이징을 바로 지우는 이유는 용량이다. 220MB짜리 덤프가 풀리면 2.4GB가 되므로
30일치를 남겨 두면 72GB가 된다. 원본 아카이브가 있으니 언제든 다시 꺼낼 수 있고,
다시 꺼내는 비용은 몇 분이다. 남겨 둘 값어치가 없다.

적재는 컨테이너에서 돈다. 호스트에는 Spark가 없고, 있으면 안 된다.
검증 환경과 실행 환경이 갈라지는 순간 "내 컴퓨터에서는 됐는데"가 시작된다.

    FLUX_STORAGE_ROOT=... python scripts/bronze_backfill.py --days 30
"""

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

from flux.bronze import backfill
from flux.common import config
from flux.ingest import catalog
from flux.ingest.catalog import DumpKind

IMAGE = "flux-spark"
CONTAINER_ROOT = "/data"

# Delta 의존 jar 87MB를 컨테이너가 뜰 때마다 다시 받는다. 30번이면 2.6GB이고
# 그만큼 공개 저장소를 두들긴다. 캐시를 이름 있는 볼륨에 두면 첫 회만 받는다.
IVY_VOLUME = "flux-ivy2"
IVY_PATH = "/opt/flux/ivy"


def find_archives(root: Path, days: int) -> list[tuple[str, Path]]:
    """받아 둔 덤프 중 최근 days일 창에 드는 것만 오래된 것부터."""
    base = root / "dumps" / "listenbrainz" / "incremental"
    on_disk = {}
    for directory in base.glob("*-incremental"):
        archives = sorted(directory.glob("listenbrainz-listens-dump-*.tar.zst"))
        if archives:
            on_disk[directory.name] = archives[0]

    dumps = [d for d in (catalog.parse_dump_name(n) for n in on_disk) if d is not None]
    wanted = catalog.recent_dumps(dumps, days)
    return [(d.name, on_disk[d.name]) for d in wanted if d.kind is DumpKind.INCREMENTAL]


def extract(step, python: str, repo: Path) -> None:
    """꺼내기는 호스트에서 돈다. 경로는 모두 절대로 준다.

    상대 경로는 부르는 쪽의 현재 디렉터리에 기대는데, 그 기대가 깨지면
    "파일을 찾을 수 없다"는 말만 남고 무엇을 못 찾았는지는 안 남는다.
    """
    subprocess.run(
        [str(Path(python).resolve()), str(repo / "scripts" / "extract_listens.py"),
         "--dump", step.name],
        check=True,
        cwd=repo,
    )


def container_tag(dump: str) -> str:
    """덤프 이름에서 날짜 부분만 뽑아 컨테이너 이름에 쓴다.

    뒤에서 잘라 내면 모든 덤프가 "remental"로 끝나 같은 이름이 된다.
    이름이 겹치면 무엇이 도는 중인지 밖에서 알 수 없다.
    """
    for part in dump.split("-"):
        if len(part) == 8 and part.isdigit():
            return part
    return dump[:12]


def ingest(step, root: Path, repo: Path) -> None:
    subprocess.run(
        [
            "docker", "run", "--rm",
            "--name", f"flux-backfill-{container_tag(step.name)}",
            "-v", f"{root}:{CONTAINER_ROOT}",
            "-v", f"{repo / 'scripts'}:/opt/flux/scripts:ro",
            "-v", f"{repo / 'src'}:/opt/flux/src:ro",
            "-v", f"{IVY_VOLUME}:{IVY_PATH}",
            "-v", f"{repo / 'docker' / 'spark-conf'}:/opt/flux/sparkconf:ro",
            "-e", "SPARK_CONF_DIR=/opt/flux/sparkconf",
            "-e", "PYTHONPATH=/opt/flux/src",
            IMAGE,
            "python", "scripts/bronze_ingest.py",
            "--root", CONTAINER_ROOT, "--dump", step.name,
        ],
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--keep-staging", action="store_true",
                        help="적재 후에도 스테이징을 남긴다. 조사할 때만 쓴다")
    parser.add_argument("--python", default=sys.executable)
    args = parser.parse_args()

    root = config.local_root()
    repo = Path(__file__).resolve().parent.parent
    archives = find_archives(root, args.days)
    done = backfill.reconciled_dumps(root / "measurements")
    steps = backfill.plan(archives, root / "staging", done=done)

    print(f"받아 둔 덤프 : {len(archives)}개")
    print(f"이미 적재됨  : {len(archives) - len(steps)}개")
    print(f"할 일        : {len(steps)}개\n")
    if not steps:
        return 0

    started = time.time()
    for i, step in enumerate(steps, 1):
        mark = time.time()
        print(f"[{i}/{len(steps)}] {step.name}", flush=True)
        extract(step, args.python, repo)
        ingest(step, root, repo)
        staged = backfill.directory_size(step.staging)
        if not args.keep_staging:
            shutil.rmtree(step.staging, ignore_errors=True)
        bronze = backfill.directory_size(root / "bronze")
        print(f"  {time.time() - mark:.0f}초  "
              f"스테이징 {backfill.format_bytes(staged)}  "
              f"Bronze 누적 {backfill.format_bytes(bronze)}\n", flush=True)

    ok = backfill.reconciled_dumps(root / "measurements")
    print(f"총 {time.time() - started:.0f}초, 대조 통과 덤프 {len(ok)}개")
    print(f"Bronze {backfill.format_bytes(backfill.directory_size(root / 'bronze'))}")
    missing = [s.name for s in steps if s.name not in ok]
    if missing:
        print(f"대조 기록이 없는 덤프 {len(missing)}개: {', '.join(missing)}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
