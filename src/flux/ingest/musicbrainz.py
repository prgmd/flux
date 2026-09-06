"""MusicBrainz 코어 덤프 카탈로그.

ListenBrainz와 구조가 다르다.
  - 최신 덤프를 `LATEST` 파일이 알려준다 (디렉터리 이름을 파싱하지 않는다)
  - 체크섬이 파일마다 붙지 않고 `SHA256SUMS` 하나에 모여 있다
"""

import re
import urllib.request
from pathlib import Path
from dataclasses import dataclass
from typing import Callable

BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/data/fullexport"

# 이 프로젝트가 쓰는 것만 받는다. edit(15GB)은 편집 이력이라 필요 없다.
WANTED = ("mbdump.tar.bz2", "mbdump-derived.tar.bz2")

_SUMS_LINE = re.compile(r"^([0-9a-fA-F]{32,64})\s+\*?(\S+)$")


class MusicBrainzError(RuntimeError):
    """MusicBrainz 카탈로그를 읽지 못했을 때 발생한다."""


Fetch = Callable[[str], str]


def _http_get(url: str, timeout: int = 60) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "flux-catalog/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


@dataclass(frozen=True)
class MBFile:
    name: str
    url: str
    md5: str | None = None
    sha256: str | None = None

    @property
    def checksums(self) -> list[tuple[str, str]]:
        pairs = []
        if self.md5:
            pairs.append(("md5", self.md5))
        if self.sha256:
            pairs.append(("sha256", self.sha256))
        return pairs


def latest_release(*, fetch: Fetch = _http_get, base: str = BASE_URL) -> str:
    """LATEST 파일이 가리키는 덤프 디렉터리 이름."""
    name = fetch(f"{base.rstrip('/')}/LATEST").strip()
    if not name or "/" in name or ".." in name:
        raise MusicBrainzError(f"LATEST 내용이 올바르지 않다: {name!r}")
    return name


def parse_sums(text: str) -> dict[str, str]:
    """`<해시>  <파일명>` 형식의 모음 파일을 파일명 기준 사전으로 바꾼다."""
    sums = {}
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        match = _SUMS_LINE.match(line)
        if match:
            sums[match.group(2)] = match.group(1).lower()
    if not sums:
        raise MusicBrainzError("체크섬 모음 파일에서 아무 줄도 읽지 못했다")
    return sums


def list_files(
    release: str | None = None,
    *,
    wanted: tuple[str, ...] = WANTED,
    fetch: Fetch = _http_get,
    base: str = BASE_URL,
) -> list[MBFile]:
    """받을 파일 목록을 체크섬과 함께 돌려준다."""
    release = release or latest_release(fetch=fetch, base=base)
    dir_url = f"{base.rstrip('/')}/{release}/"

    md5s, sha256s = {}, {}
    for name, target in (("MD5SUMS", md5s), ("SHA256SUMS", sha256s)):
        try:
            target.update(parse_sums(fetch(dir_url + name)))
        except (MusicBrainzError, OSError):
            pass  # 한쪽이 없어도 다른 쪽으로 검증한다

    if not md5s and not sha256s:
        raise MusicBrainzError(f"체크섬 파일을 하나도 읽지 못했다: {dir_url}")

    files = []
    for name in wanted:
        files.append(
            MBFile(
                name=name,
                url=dir_url + name,
                md5=md5s.get(name),
                sha256=sha256s.get(name),
            )
        )
    return files


def in_progress_releases(dumps_dir) -> list[str]:
    """받다 만 조각이 남아 있는 릴리스 목록. 최신 순.

    LATEST는 며칠에 한 번 교체된다. 여러 날에 걸치는 다운로드가
    매번 LATEST를 다시 물어보면 새 디렉터리에 처음부터 받게 된다.
    """
    base = Path(dumps_dir)
    if not base.is_dir():
        return []
    found = {
        part.parent.name
        for part in base.glob("*/*.part")
        if part.stat().st_size > 0
    }
    return sorted(found, reverse=True)


def resolve_release(
    dumps_dir,
    requested: str | None = None,
    *,
    fetch: Fetch = _http_get,
    base: str = BASE_URL,
) -> tuple[str, str | None]:
    """받을 릴리스를 정한다. (릴리스, 경고문) 을 돌려준다.

    - 명시적으로 지정하면 그대로 쓴다
    - 받다 만 것이 있으면 그것을 이어받는다 (LATEST가 바뀌었어도)
    - 없으면 LATEST를 따른다
    """
    if requested:
        return requested, None

    latest = latest_release(fetch=fetch, base=base)
    pending = in_progress_releases(dumps_dir)
    if pending and latest not in pending:
        chosen = pending[0]
        return chosen, (
            f"받다 만 조각이 있어 {chosen}을 이어받는다. "
            f"서버의 최신은 {latest}이다. 최신을 받으려면 --release {latest}를 준다."
        )
    return latest, None
