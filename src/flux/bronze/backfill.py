"""여러 날치를 이어서 적재할 때 무엇을 건너뛰고 무엇을 할지 정한다.

적재 자체는 멱등이므로 다시 돌려도 결과는 같다. 그래도 건너뛰기가 필요한 이유는
정확성이 아니라 시간이다. 하루치 적재가 1분이면 30일은 30분이고, 중간에 끊겨
처음부터 다시 돌리면 그 시간을 통째로 다시 쓴다.

판단 근거는 적재가 남긴 대조 기록이다. 대조를 통과하지 못한 기록은 없는 것으로
본다. 실패한 적재를 건너뛰면 구멍이 그대로 남기 때문이다.
"""

import json
from dataclasses import dataclass
from pathlib import Path

MEASUREMENT_PREFIX = "bronze-"
MEASUREMENT_SUFFIX = ".json"


@dataclass(frozen=True)
class Step:
    """덤프 하나를 처리하는 한 걸음."""

    name: str
    staging: Path


def measurement_path(measurements_dir, dump: str) -> Path:
    return Path(measurements_dir) / f"{MEASUREMENT_PREFIX}{dump}{MEASUREMENT_SUFFIX}"


def reconciled_dumps(measurements_dir) -> set[str]:
    """대조를 통과한 기록이 남은 덤프 이름.

    깨진 파일은 조용히 무시한다. 기록을 못 읽는 것은 적재를 다시 하면 되는 일이고,
    여기서 예외를 올리면 남은 29일치가 통째로 멈춘다.
    """
    directory = Path(measurements_dir)
    if not directory.is_dir():
        return set()

    done = set()
    for path in directory.glob(f"{MEASUREMENT_PREFIX}*{MEASUREMENT_SUFFIX}"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict) or payload.get("reconciled") is not True:
            continue
        name = payload.get("dump")
        if isinstance(name, str) and name:
            done.add(name)
    return done


def plan(archives, staging_root, *, done=()) -> list[Step]:
    """받아 둔 덤프 아카이브에서 아직 적재하지 않은 것만 걸음으로 만든다.

    archives는 (덤프 이름, tar 경로) 쌍이다. 경로는 꺼내기가 알아서 찾으므로
    걸음에 담지 않는다. 이름은 덤프 디렉터리 이름이고
    적재의 `_source_dump`와 같아야 한다. 두 곳에서 다른 이름을 쓰면
    멱등 교체가 엉뚱한 구획을 지운다.
    """
    finished = set(done)
    root = Path(staging_root)
    steps = []
    for name, _archive in archives:
        if name in finished:
            continue
        steps.append(Step(name=name, staging=root / name))
    return steps


def directory_size(path) -> int:
    """디렉터리가 쓰는 바이트. 없으면 0."""
    directory = Path(path)
    if not directory.exists():
        return 0
    return sum(f.stat().st_size for f in directory.rglob("*") if f.is_file())


def format_bytes(size: int) -> str:
    if size >= 1024 ** 3:
        return f"{size / 1024 ** 3:.2f}GB"
    if size >= 1024 ** 2:
        return f"{size / 1024 ** 2:.0f}MB"
    return f"{size:,}B"


def container_tag(dump: str) -> str:
    """덤프 이름에서 날짜 부분만 뽑아 컨테이너 이름에 쓴다.

    뒤에서 잘라 내면 모든 덤프가 "remental"로 끝나 같은 이름이 된다.
    이름이 겹치면 무엇이 도는 중인지 밖에서 알 수 없다.

    처음엔 스크립트(`scripts/pipeline/bronze_backfill.py`) 안에 있었고, 테스트가
    `sys.path`에 `scripts`를 끼워 넣어 그것을 빌려 썼다. 테스트할 판단은 스크립트가
    아니라 모듈에 둔다. 스크립트 폴더를 옮기자 그 테스트가 깨질 뻔했다.
    """
    for part in dump.split("-"):
        if len(part) == 8 and part.isdigit():
            return part
    return dump[:12]
