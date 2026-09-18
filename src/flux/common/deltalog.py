"""Spark 없이 Delta 테이블의 활성 파일 목록을 읽는다.

정합성 오라클에 필요하다. **Spark가 낸 집계를 Spark로 검증할 수는 없으므로**
다른 엔진으로 같은 것을 세야 하는데, 그 엔진에게 "어느 parquet을 읽어라"를
알려주려면 트랜잭션 로그를 읽어야 한다.

## 폴더를 glob하면 왜 안 되는가

Delta 테이블 폴더에는 **지금 테이블에 속하지 않는 parquet이 섞여 있다.**

    덮어쓰기로 교체된 옛 파일     로그에서 remove됐지만 디스크에는 남는다
    강제 종료가 남긴 고아 파일    커밋이 없어 로그에 아예 없다

실제로 Bronze 적재를 도중에 죽였을 때 고아 parquet 44개가 남았고, 그것들이
읽히지 않는 것이 Delta ACID의 증거였다. **폴더를 glob하면 그 44개를 같이
읽는다.** 오라클이 틀린 답을 들고 Spark를 의심하게 된다.

## 무엇이 진실인가

`_delta_log/`의 JSON 커밋들이다. 한 줄에 동작 하나가 적혀 있다.

    {"add":    {"path": "part-0000...", "partitionValues": {...}, ...}}
    {"remove": {"path": "part-0000...", ...}}

버전 순서대로 읽으며 add는 넣고 remove는 뺀다. 남은 것이 지금 테이블이다.
git이 커밋을 쌓아 현재 트리를 만드는 것과 같은 구조다.

pyspark를 부르지 않는다. 표준 라이브러리만 쓴다.
"""

import json
from pathlib import Path

LOG_DIR = "_delta_log"
CHECKPOINT_SUFFIX = ".checkpoint.parquet"


LAST_CHECKPOINT = "_last_checkpoint"


class CheckpointMissing(Exception):
    """체크포인트를 가리키는데 그 파일이 없다.

    조용히 JSON만 읽으면 **오라클이 데이터의 일부만 보고 불일치를 보고한다.**
    그건 Spark가 틀렸다는 뜻이 아니라 오라클이 덜 읽은 것이므로 막는다.
    """


def _version_of(path: Path) -> int:
    return int(path.name.split(".")[0])


def checkpoint_version(log: Path):
    """마지막 체크포인트 버전. 없으면 None.

    Delta는 커밋이 쌓이면 열 개마다 그 시점의 상태를 parquet 하나로 접는다.
    그래야 커밋이 천 개가 돼도 읽기가 천 개를 훑지 않는다. git의 팩 파일과
    같은 발상이다.
    """
    marker = log / LAST_CHECKPOINT
    if not marker.is_file():
        return None
    try:
        return json.loads(marker.read_text(encoding="utf-8"))["version"]
    except (ValueError, KeyError):
        return None


def _checkpoint_adds(log: Path, version: int) -> dict:
    """체크포인트가 담은 활성 파일. 그 시점의 add가 곧 살아 있는 파일이다.

    parquet이라 표준 라이브러리로는 못 읽는다. 오라클이 이미 쓰는 duckdb를
    빌려 쓰고, **여기서만** import한다. 이 모듈을 읽는 것만으로 duckdb가
    필요해지면 곤란하다.
    """
    path = log / f"{version:020d}{CHECKPOINT_SUFFIX}"
    if not path.is_file():
        raise CheckpointMissing(f"체크포인트가 없다: {path}")

    import duckdb

    rows = duckdb.sql(
        "SELECT add.path AS path, add.partitionValues AS parts "
        f"FROM read_parquet('{path.as_posix()}') WHERE add IS NOT NULL"
    ).fetchall()
    return {path_: dict(parts or {}) for path_, parts in rows}


def commits(table: Path, *, after: int = -1):
    """버전 순서대로 커밋 파일 경로. `after`보다 큰 것만."""
    log = Path(table) / LOG_DIR
    if not log.is_dir():
        raise FileNotFoundError(f"Delta 로그가 없다: {log}")
    return [p for p in sorted(log.glob("*.json"))
            if p.name[0].isdigit() and _version_of(p) > after]


def actions(path: Path):
    """커밋 하나의 동작들. 깨진 줄은 건너뛴다."""
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError:
                continue


def active_files(table) -> list:
    """지금 테이블에 속한 parquet의 상대 경로.

    add와 remove를 버전 순서대로 적용한다. 같은 경로가 다시 add되면
    최신 것이 이긴다.
    """
    return sorted(partition_values(table))


def active_paths(table) -> list:
    """활성 파일의 절대 경로. 다른 엔진에 그대로 넘긴다."""
    table = Path(table)
    return [str(table / name) for name in active_files(table)]


def partition_values(table) -> dict:
    """활성 파일마다의 파티션 값. 읽을 파일을 미리 줄이는 데 쓴다.

    파티션 컬럼은 parquet 안에 없고 **경로에만** 있다. Spark는 경로에서
    복원해 주지만 다른 엔진은 그러지 않으므로, 필요하면 여기서 꺼내 쓴다.
    """
    table = Path(table)
    log = table / LOG_DIR
    if not log.is_dir():
        raise FileNotFoundError(f"Delta 로그가 없다: {log}")

    version = checkpoint_version(log)
    alive = _checkpoint_adds(log, version) if version is not None else {}

    for commit in commits(table, after=version if version is not None else -1):
        for action in actions(commit):
            if "add" in action:
                entry = action["add"]
                alive[entry["path"]] = entry.get("partitionValues") or {}
            elif "remove" in action:
                alive.pop(action["remove"].get("path"), None)
    return alive


def files_in_partitions(table, column: str, values) -> list:
    """파티션 값이 주어진 것들 중 하나인 파일만.

    한 주를 재려고 테이블 전체를 읽을 이유가 없다. 260개 파티션 중
    두어 개만 읽으면 된다.
    """
    wanted = set(values)
    table = Path(table)
    return [
        str(table / name)
        for name, parts in sorted(partition_values(table).items())
        if parts.get(column) in wanted
    ]
