import re
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from typing import Callable, Iterable

BASE_URL = "https://data.metabrainz.org/pub/musicbrainz/listenbrainz"

_HREF = re.compile(r'href="([^"]+)"', re.IGNORECASE)

# listenbrainz-dump-2651-20260904-000002-incremental
_DUMP_DIR = re.compile(
    r"^listenbrainz-dump-(?P<serial>\d+)-(?P<date>\d{8})-(?P<time>\d{6})-"
    r"(?P<kind>full|incremental)/?$"
)

CHECKSUM_SUFFIXES = (".md5", ".sha256")


class CatalogError(RuntimeError):
    """카탈로그를 읽거나 해석하지 못했을 때 발생한다."""


class DumpKind(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"

    @property
    def listing_path(self) -> str:
        return "fullexport" if self is DumpKind.FULL else "incremental"


@dataclass(frozen=True, order=True)
class Dump:
    serial: int
    dumped_at: datetime
    kind: DumpKind
    name: str

    @property
    def dumped_on(self) -> date:
        return self.dumped_at.date()

    def url(self, base: str = BASE_URL) -> str:
        return f"{base.rstrip('/')}/{self.kind.listing_path}/{self.name}/"


@dataclass(frozen=True)
class DumpFile:
    name: str
    url: str
    md5_url: str | None = None
    sha256_url: str | None = None

    @property
    def has_checksum(self) -> bool:
        return self.md5_url is not None or self.sha256_url is not None


def _http_get(url: str, timeout: int = 60) -> str:
    request = urllib.request.Request(url, headers={"User-Agent": "flux-catalog/0.1"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


Fetch = Callable[[str], str]


def parse_links(html: str) -> list[str]:
    """디렉터리 목록 HTML에서 링크만 뽑는다. 상위 디렉터리는 제외한다."""
    return [href for href in _HREF.findall(html) if href not in ("../", "..", "/")]


def parse_dump_name(name: str) -> Dump | None:
    """덤프 디렉터리 이름을 구조화한다. 형식이 다르면 None."""
    match = _DUMP_DIR.match(name.strip())
    if match is None:
        return None
    try:
        dumped_at = datetime.strptime(
            match.group("date") + match.group("time"), "%Y%m%d%H%M%S"
        )
    except ValueError:
        return None
    return Dump(
        serial=int(match.group("serial")),
        dumped_at=dumped_at,
        kind=DumpKind(match.group("kind")),
        name=match.group(0).rstrip("/"),
    )


def list_dumps(
    kind: DumpKind,
    *,
    fetch: Fetch = _http_get,
    base: str = BASE_URL,
) -> list[Dump]:
    """해당 종류의 덤프 목록을 오래된 것부터 돌려준다."""
    url = f"{base.rstrip('/')}/{kind.listing_path}/"
    html = fetch(url)
    dumps = []
    for link in parse_links(html):
        dump = parse_dump_name(link)
        if dump is not None and dump.kind is kind:
            dumps.append(dump)
    return sorted(dumps)


def latest_dump(
    kind: DumpKind,
    *,
    fetch: Fetch = _http_get,
    base: str = BASE_URL,
) -> Dump:
    dumps = list_dumps(kind, fetch=fetch, base=base)
    if not dumps:
        raise CatalogError(f"{kind.value} 덤프를 하나도 찾지 못했다")
    return dumps[-1]


def _split_checksums(links: Iterable[str]) -> tuple[list[str], dict[str, str]]:
    payloads, checksums = [], {}
    for link in links:
        name = link.rstrip("/")
        for suffix in CHECKSUM_SUFFIXES:
            if name.endswith(suffix):
                checksums[name] = suffix
                break
        else:
            payloads.append(name)
    return payloads, checksums


def list_files(
    dump: Dump,
    *,
    fetch: Fetch = _http_get,
    base: str = BASE_URL,
) -> list[DumpFile]:
    """덤프 안의 실제 파일 목록. 체크섬 파일은 본 파일에 묶어서 돌려준다."""
    dir_url = dump.url(base)
    payloads, checksums = _split_checksums(parse_links(fetch(dir_url)))

    files = []
    for name in sorted(payloads):
        md5 = f"{name}.md5" if f"{name}.md5" in checksums else None
        sha = f"{name}.sha256" if f"{name}.sha256" in checksums else None
        files.append(
            DumpFile(
                name=name,
                url=dir_url + name,
                md5_url=dir_url + md5 if md5 else None,
                sha256_url=dir_url + sha if sha else None,
            )
        )
    return files


def find_file(files: Iterable[DumpFile], keyword: str) -> DumpFile:
    """이름에 keyword가 들어간 파일 하나를 고른다. 없거나 여럿이면 실패시킨다."""
    matches = [f for f in files if keyword in f.name]
    if not matches:
        raise CatalogError(f"'{keyword}'를 포함한 파일이 없다")
    if len(matches) > 1:
        names = ", ".join(f.name for f in matches)
        raise CatalogError(f"'{keyword}'에 여러 파일이 걸린다: {names}")
    return matches[0]
