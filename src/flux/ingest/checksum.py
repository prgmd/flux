import hashlib
import re
from pathlib import Path
from typing import Iterable

CHUNK_SIZE = 1024 * 1024

ALGORITHMS = {"md5": 32, "sha256": 64}

_HEX = re.compile(r"^[0-9a-f]+$")


class ChecksumError(RuntimeError):
    """체크섬을 읽지 못했거나 대조에 실패했을 때 발생한다."""


class ChecksumMismatch(ChecksumError):
    """계산한 해시가 기대값과 다를 때 발생한다."""

    def __init__(self, path, algorithm, expected, actual):
        self.path = path
        self.algorithm = algorithm
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"{Path(path).name}의 {algorithm} 불일치\n"
            f"  기대: {expected}\n"
            f"  실제: {actual}"
        )


def parse_checksum(text: str, algorithm: str) -> str:
    """체크섬 파일 내용에서 16진수 해시를 뽑는다.

    ListenBrainz는 해시만 넣어 주지만, `<해시>  <파일명>` 형식도 함께 받아들인다.
    """
    if algorithm not in ALGORITHMS:
        raise ChecksumError(f"모르는 알고리즘: {algorithm}")

    token = text.strip().split()
    if not token:
        raise ChecksumError(f"{algorithm} 파일이 비어 있다")

    digest = token[0].lower()
    width = ALGORITHMS[algorithm]
    if len(digest) != width or not _HEX.match(digest):
        raise ChecksumError(
            f"{algorithm} 형식이 아니다 (16진수 {width}자여야 한다): {token[0]!r}"
        )
    return digest


def hash_file(path, algorithm: str, *, chunk_size: int = CHUNK_SIZE) -> str:
    """파일을 조금씩 읽어 해시를 계산한다. 전체를 메모리에 올리지 않는다."""
    if algorithm not in ALGORITHMS:
        raise ChecksumError(f"모르는 알고리즘: {algorithm}")

    digest = hashlib.new(algorithm)
    with open(path, "rb") as fp:
        while chunk := fp.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def verify(path, algorithm: str, expected: str, *, chunk_size: int = CHUNK_SIZE) -> str:
    """해시를 계산해 기대값과 대조한다. 다르면 즉시 실패시킨다."""
    expected = parse_checksum(expected, algorithm)
    actual = hash_file(path, algorithm, chunk_size=chunk_size)
    if actual != expected:
        raise ChecksumMismatch(path, algorithm, expected, actual)
    return actual


def verify_all(path, expectations: Iterable[tuple[str, str]]) -> dict[str, str]:
    """여러 알고리즘을 한 번에 대조한다. 하나라도 어긋나면 실패한다."""
    results = {}
    for algorithm, expected in expectations:
        results[algorithm] = verify(path, algorithm, expected)
    if not results:
        raise ChecksumError("대조할 체크섬이 하나도 주어지지 않았다")
    return results
