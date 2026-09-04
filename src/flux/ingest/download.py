import os
import re
import time
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


_CONTENT_RANGE = re.compile(r"bytes\s+(\d+)-(\d+)/(\d+)")


def parse_total_size(headers, status: int) -> int | None:
    """응답 헤더에서 파일 전체 크기를 뽑는다.

    206 응답의 Content-Length는 '이번에 보내는 양'이지 전체가 아니다.
    전체는 Content-Range의 슬래시 뒤에 있다.
    """
    if status == 206:
        match = _CONTENT_RANGE.search(headers.get("Content-Range", "") or "")
        return int(match.group(3)) if match else None
    length = headers.get("Content-Length")
    return int(length) if length is not None else None


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
    resume: bool = True,
    opener=_open,
) -> Path:
    """URL을 파일로 내려받는다. 중단된 조각이 있으면 이어받는다.

    받는 동안에는 `.part` 파일에 쓰고, 다 받은 뒤에만 최종 이름으로 바꾼다.
    중단된 파일이 완성본으로 오인되지 않게 하기 위해서다.
    """
    dest = Path(dest)
    if dest.exists() and not overwrite:
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_name(dest.name + PART_SUFFIX)

    already = part.stat().st_size if (resume and part.exists()) else 0
    headers = {"Range": f"bytes={already}-"} if already else None

    with opener(url, timeout, headers) as response:
        status = getattr(response, "status", 200)
        if already and status != 206:
            # 서버가 Range를 무시했다. 처음부터 다시 받는다.
            already = 0
        total = parse_total_size(response.headers, status)

        received = already
        mode = "ab" if already else "wb"
        with open(part, mode) as fp:
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


def download_with_retry(
    url: str,
    dest,
    *,
    attempts: int = 3,
    backoff: float = 2.0,
    sleep=time.sleep,
    **kwargs,
) -> Path:
    """끊기면 이어받으며 다시 시도한다.

    이어받기가 있으므로 재시도는 처음부터가 아니라 남은 부분부터다.
    크기 불일치는 조각을 지우므로 재시도해도 같은 자리에서 반복되지 않는다.
    """
    last = None
    for attempt in range(1, attempts + 1):
        try:
            return download(url, dest, **kwargs)
        except (OSError, DownloadError) as exc:
            last = exc
            if attempt == attempts:
                break
            sleep(backoff ** (attempt - 1))
    raise DownloadError(f"{attempts}번 시도 후 실패: {last}") from last


def format_progress(received: int, total: int | None) -> str:
    mb = received / 1024 / 1024
    if total is None:
        return f"{mb:,.1f}MB"
    return f"{mb:,.1f}MB / {total / 1024 / 1024:,.1f}MB ({received * 100 // total}%)"
