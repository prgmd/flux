import json
import os
import re
from pathlib import Path
from urllib.parse import urlsplit

STORAGE_ROOT_ENV = "FLUX_STORAGE_ROOT"

_SCHEME = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.\-]*://")
_WINDOWS_DRIVE = re.compile(r"^[a-zA-Z]:[\\/]")
# file:///C:/data 의 경로는 /C:/data 이고 앞 슬래시를 떼야 한다.
# file:///data 의 /data 에서 떼면 상대 경로가 되므로 드라이브 문자가 뒤따를 때만 뗀다.
_DRIVE_PREFIX = re.compile(r"^/[a-zA-Z]:(/|$)")

BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"
DUMPS = "dumps"
SCRATCH = "scratch"


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


def scratch(*segments: str) -> str:
    """드릴이 운영 테이블 대신 쓰는 임시 사본의 자리.

    드릴은 운영 테이블을 바꿨다가 시점 조회로 되돌리는 방식이었다. Delta는 지운
    파일 보존 기간(기본 7일)보다 오래된 버전으로의 시점 조회를 막으므로, 차원이
    한동안 안 바뀌었으면 **되돌리기가 실패하고 운영 테이블이 바뀐 채로 남는다.**
    실제로 placeholder 승격 드릴이 그렇게 끝났다. 사본에서 하면 되돌릴 것이 없다.
    """
    return path(SCRATCH, *segments)


def is_local() -> bool:
    return urlsplit(storage_root()).scheme == "file"


def local_root() -> Path:
    """저장 루트를 실제 파일 시스템 경로로 돌려준다. 로컬이 아니면 실패시킨다.

    다운로드나 압축 해제처럼 일반 파일 입출력이 필요한 단계에서 쓴다.
    Spark가 읽고 쓰는 경로는 이 함수가 아니라 bronze()/silver()/gold()를 쓴다.
    스킴이 있는 URI를 그대로 넘겨야 하기 때문이다.

    `file:///C:/data`의 경로 부분은 `/C:/data`이고 앞 슬래시를 떼야 하지만,
    `file:///data`의 `/data`에서 떼면 상대 경로가 된다. 드라이브 문자가
    뒤따를 때만 뗀다.
    """
    root = storage_root()
    if not is_local():
        raise StorageConfigError(
            f"로컬 경로가 아니다: {root}. 이 단계는 file:// 만 지원한다."
        )
    raw = urlsplit(root).path
    if _DRIVE_PREFIX.match(raw):
        raw = raw[1:]
    return Path(raw)


MEASUREMENTS = "measurements"


def write_measurement(name: str, payload: dict) -> Path:
    """측정 결과를 데이터 루트의 measurements/에 남긴다.

    같은 다섯 줄이 열일곱 스크립트에 복사돼 있었다. 저장 위치나 형식을 바꾸려면
    열일곱 곳을 고쳐야 했고, 그중 하나를 빠뜨리면 그 기록만 다른 자리에 남는다.

    한글이 그대로 보이도록 ensure_ascii를 끈다. 나중에 사람이 열어 볼 파일이다.
    """
    directory = local_root() / MEASUREMENTS
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{name}.json"
    target.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return target
