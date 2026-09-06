"""MusicBrainz 코어 덤프를 풀지 않고 훑는다.

mbdump.tar.bz2는 압축을 풀면 수십 GB가 된다. 행 수를 세거나 몇 줄을 엿보는
정도라면 디스크에 풀 이유가 없다. 스트리밍으로 읽으면서 필요한 것만 취한다.

덤프 안의 테이블은 탭으로 구분된 텍스트이고, 한 줄이 한 행이다.
"""

import tarfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator

CHUNK_SIZE = 4 * 1024 * 1024

# 덤프 안에서 테이블은 mbdump/ 아래에 확장자 없이 들어 있다.
TABLE_PREFIX = "mbdump/"


class MBDumpError(RuntimeError):
    """덤프를 읽지 못했을 때 발생한다."""


@dataclass
class TableStats:
    name: str
    rows: int = 0
    bytes_read: int = 0
    sample: list[str] = field(default_factory=list)

    @property
    def avg_row_bytes(self) -> float:
        return round(self.bytes_read / self.rows, 1) if self.rows else 0.0


def table_name(member_name: str) -> str | None:
    """tar 멤버 이름에서 테이블 이름을 뽑는다. 테이블이 아니면 None."""
    if not member_name.startswith(TABLE_PREFIX):
        return None
    rest = member_name[len(TABLE_PREFIX):]
    if not rest or "/" in rest:
        return None  # 하위 디렉터리나 디렉터리 자체는 테이블이 아니다
    return rest


def count_rows(stream, *, chunk_size: int = CHUNK_SIZE) -> tuple[int, int]:
    """줄 수와 바이트 수를 센다. 전체를 메모리에 올리지 않는다.

    마지막 줄에 개행이 없을 수 있으므로 그 경우를 따로 더한다.
    """
    rows = 0
    total = 0
    trailing = False
    while chunk := stream.read(chunk_size):
        total += len(chunk)
        rows += chunk.count(b"\n")
        trailing = not chunk.endswith(b"\n")
    if trailing:
        rows += 1
    return rows, total


def read_tables(
    dump_path,
    wanted: Iterable[str] | None = None,
    *,
    sample_rows: int = 0,
    chunk_size: int = CHUNK_SIZE,
) -> Iterator[TableStats]:
    """덤프를 훑으며 테이블별 통계를 하나씩 내놓는다.

    wanted를 주면 그 테이블만 센다. 다 찾으면 즉시 멈춘다.
    7GB를 끝까지 읽지 않아도 되는 경우가 대부분이다.
    """
    path = Path(dump_path)
    if not path.exists():
        raise MBDumpError(f"덤프가 없다: {path}")

    remaining = set(wanted) if wanted else None

    with tarfile.open(path, mode="r|bz2") as tar:
        for member in tar:
            if not member.isfile():
                continue
            name = table_name(member.name)
            if name is None:
                continue
            if remaining is not None and name not in remaining:
                continue

            stream = tar.extractfile(member)
            stats = TableStats(name=name)

            if sample_rows:
                for _ in range(sample_rows):
                    line = stream.readline()
                    if not line:
                        break
                    stats.rows += 1
                    stats.bytes_read += len(line)
                    stats.sample.append(line.decode("utf-8", errors="replace").rstrip("\n"))

            rows, read_bytes = count_rows(stream, chunk_size=chunk_size)
            stats.rows += rows
            stats.bytes_read += read_bytes
            yield stats

            if remaining is not None:
                remaining.discard(name)
                if not remaining:
                    return
