import os
import re
from urllib.parse import urlsplit

STORAGE_ROOT_ENV = "FLUX_STORAGE_ROOT"

_SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://")
_WINDOWS_DRIVE = re.compile(r"^[a-zA-Z]:[\\/]")

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
DUMPS = "dumps"


class StorageConfigError(RuntimeError):
    """저장 경로 설정이 잘못되었을 때 발생한다."""


def _to_uri(raw: str) -> str:
    value = raw.strip()
    if not value:
        raise StorageConfigError(f"{STORAGE_ROOT_ENV}가 비어 있다")

    if _WINDOWS_DRIVE.match(value):
        return "file:///" + value.replace("\\", "/").lstrip("/")

    if value.startswith("/"):
        return "file://" + value

    if not _SCHEME.match(value):
        raise StorageConfigError(
            f"{STORAGE_ROOT_ENV}는 스킴이 있는 URI나 절대 경로여야 한다: {raw!r}"
        )

    return value


def _normalize(uri: str) -> str:
    parts = urlsplit(uri)
    if not parts.scheme:
        raise StorageConfigError(f"스킴이 없다: {uri!r}")
    path = parts.path.rstrip("/")
    netloc = parts.netloc
    return f"{parts.scheme}://{netloc}{path}"


def storage_root() -> str:
    raw = os.environ.get(STORAGE_ROOT_ENV)
    if raw is None:
        raise StorageConfigError(
            f"{STORAGE_ROOT_ENV}가 설정되지 않았다. "
            "데이터 루트는 저장소 밖의 경로여야 하므로 기본값을 두지 않는다."
        )
    return _normalize(_to_uri(raw))


def _check_segment(segment: str) -> str:
    value = str(segment).strip()
    if not value:
        raise StorageConfigError("빈 경로 조각은 쓸 수 없다")
    if value.startswith("/") or value.endswith("/"):
        raise StorageConfigError(f"경로 조각에 앞뒤 슬래시를 넣지 않는다: {segment!r}")
    for piece in value.split("/"):
        if piece in ("", ".", ".."):
            raise StorageConfigError(f"경로 조각이 올바르지 않다: {segment!r}")
    return value


def path(*segments: str) -> str:
    root = storage_root()
    if not segments:
        return root
    tail = "/".join(_check_segment(s) for s in segments)
    return f"{root}/{tail}"


def bronze(table: str) -> str:
    return path(BRONZE, table)


def silver(table: str) -> str:
    return path(SILVER, table)


def gold(table: str) -> str:
    return path(GOLD, table)


def dumps(*segments: str) -> str:
    return path(DUMPS, *segments)


def is_local() -> bool:
    return urlsplit(storage_root()).scheme == "file"
