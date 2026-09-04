import os
import urllib.request
from pathlib import Path
from typing import Callable

CHUNK_SIZE = 1024 * 1024
PART_SUFFIX = ".part"
USER_AGENT = "flux-downloader/0.1"


class DownloadError(RuntimeError):
    """다운로드에 실패했을 때 발생한다."""


class SizeMismatch(DownloadError):
    """받은 바이트 수가 서버가 알려준 크기와 다를 때 발생한다."""


Progress = Callable[[int, int | None], None]


def _open(url: str, timeout: int, headers: dict | None = None):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    return urllib.request.urlopen(request, timeout=timeout)


def remote_size(url: str, *, timeout: int = 60) -> int | None:
    """서버가 알려주는 파일 크기. 모르면 None."""
    request = urllib.request.Request(
        url, method="HEAD", headers={"User-Agent": USER_AGENT}
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        length = response.headers.get("Content-Length")
    return int(length) if length is not None else None


def download(
    url: str,
    dest,
    *,
    chunk_size: int = CHUNK_SIZE,
    timeout: int = 300,
    progress: Progress | None = None,
    overwrite: bool = False,
    opener=_open,
) -> Path:
    """URL을 파일로 내려받는다.

    받는 동안에는 `.part` 파일에 쓰고, 다 받은 뒤에만 최종 이름으로 바꾼다.
    중단된 파일이 완성본으로 오인되지 않게 하기 위해서다.
    """
    dest = Path(dest)
    if dest.exists() and not overwrite:
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + PART_SUFFIX)

    received = 0
    with opener(url, timeout) as response:
        header = response.headers.get("Content-Length")
        total = int(header) if header is not None else None
        with open(part, "wb") as fp:
            while chunk := response.read(chunk_size):
                fp.write(chunk)
                received += len(chunk)
                if progress is not None:
                    progress(received, total)

    if total is not None and received != total:
        part.unlink(missing_ok=True)
        raise SizeMismatch(
            f"받은 크기가 다르다: 기대 {total:,}바이트, 실제 {received:,}바이트"
        )

    os.replace(part, dest)
    return dest


def format_progress(received: int, total: int | None) -> str:
    mb = received / 1024 / 1024
    if total is None:
        return f"{mb:,.1f}MB"
    return f"{mb:,.1f}MB / {total / 1024 / 1024:,.1f}MB ({received * 100 // total}%)"
