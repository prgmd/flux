from datetime import datetime
from pathlib import Path

import pytest

from flux.ingest import catalog
from flux.ingest.catalog import CatalogError, Dump, DumpKind

FIXTURES = Path(__file__).parent / "fixtures"


def fixture(name):
    return (FIXTURES / name).read_text(encoding="utf-8")


@pytest.fixture
def fake_fetch():
    """URL에 따라 미리 저장한 목록 HTML을 돌려준다. 네트워크를 타지 않는다."""
    # 더 구체적인 패턴을 먼저 본다. 덤프 디렉터리 URL도 "incremental/"로 끝나기 때문이다.
    pages = [
        ("-incremental/", fixture("dump_dir_listing.html")),
        ("/incremental/", fixture("incremental_listing.html")),
        ("/fullexport/", fixture("fullexport_listing.html")),
    ]

    def _fetch(url):
        for suffix, html in pages:
            if url.endswith(suffix):
                return html
        raise AssertionError(f"준비되지 않은 URL: {url}")

    return _fetch


# ── 이름 파싱 ────────────────────────────────────────────────

def test_증분_덤프_이름을_구조화한다():
    dump = catalog.parse_dump_name("listenbrainz-dump-2651-20260904-000002-incremental/")
    assert dump == Dump(
        serial=2651,
        dumped_at=datetime(2026, 9, 4, 0, 0, 2),
        kind=DumpKind.INCREMENTAL,
        name="listenbrainz-dump-2651-20260904-000002-incremental",
    )
    assert dump.dumped_on.isoformat() == "2026-09-04"


def test_full_덤프_이름을_구조화한다():
    dump = catalog.parse_dump_name("listenbrainz-dump-2647-20260901-000002-full/")
    assert dump.kind is DumpKind.FULL
    assert dump.serial == 2647


@pytest.mark.parametrize(
    "bad",
    [
        "../",
        "labs/",
        "listenbrainz-feedback-20260904-021502-full/",
        "listenbrainz-dump-2651-20260904-000002-sample/",
        "listenbrainz-dump-abc-20260904-000002-full/",
        "listenbrainz-dump-2651-20261332-000002-full/",
        "",
    ],
)
def test_형식이_다른_이름은_None을_돌려준다(bad):
    assert catalog.parse_dump_name(bad) is None


# ── 목록 조회 ────────────────────────────────────────────────

def test_증분_목록을_읽는다(fake_fetch):
    dumps = catalog.list_dumps(DumpKind.INCREMENTAL, fetch=fake_fetch)
    assert dumps, "덤프를 하나도 못 읽었다"
    assert all(d.kind is DumpKind.INCREMENTAL for d in dumps)


def test_오래된_것부터_정렬한다(fake_fetch):
    dumps = catalog.list_dumps(DumpKind.INCREMENTAL, fetch=fake_fetch)
    assert [d.serial for d in dumps] == sorted(d.serial for d in dumps)


def test_일련번호가_건너뛰어도_최신을_고른다(fake_fetch):
    """실제 서버에서 2646 다음이 2649다. 연속을 가정하면 안 된다."""
    dumps = catalog.list_dumps(DumpKind.INCREMENTAL, fetch=fake_fetch)
    latest = catalog.latest_dump(DumpKind.INCREMENTAL, fetch=fake_fetch)
    assert latest == max(dumps)
    assert latest.serial == max(d.serial for d in dumps)


def _listing(*names):
    return "".join(f'<a href="{n}">{n}</a>' for n in names)


def test_한_목록에_두_종류가_섞여_있어도_걸러낸다():
    """서버가 종류를 섞어 내려줄 경우를 대비한다. 경로만 믿지 않는다."""
    html = _listing(
        "listenbrainz-dump-2650-20260903-000003-incremental/",
        "listenbrainz-dump-2647-20260901-000002-full/",
        "listenbrainz-dump-2651-20260904-000002-incremental/",
    )
    inc = catalog.list_dumps(DumpKind.INCREMENTAL, fetch=lambda url: html)
    full = catalog.list_dumps(DumpKind.FULL, fetch=lambda url: html)
    assert [d.serial for d in inc] == [2650, 2651]
    assert [d.serial for d in full] == [2647]


def test_역순으로_내려와도_정렬한다():
    """서버 목록 순서에 기대지 않는다."""
    html = _listing(
        "listenbrainz-dump-2651-20260904-000002-incremental/",
        "listenbrainz-dump-2646-20260901-000002-incremental/",
        "listenbrainz-dump-2649-20260902-000002-incremental/",
    )
    dumps = catalog.list_dumps(DumpKind.INCREMENTAL, fetch=lambda url: html)
    assert [d.serial for d in dumps] == [2646, 2649, 2651]
    assert catalog.latest_dump(DumpKind.INCREMENTAL, fetch=lambda url: html).serial == 2651


def test_full_목록에는_증분이_섞이지_않는다(fake_fetch):
    dumps = catalog.list_dumps(DumpKind.FULL, fetch=fake_fetch)
    assert dumps
    assert all(d.kind is DumpKind.FULL for d in dumps)


def test_덤프가_없으면_실패시킨다():
    with pytest.raises(CatalogError):
        catalog.latest_dump(DumpKind.FULL, fetch=lambda url: "<html></html>")


# ── 파일 목록 ────────────────────────────────────────────────

@pytest.fixture
def files(fake_fetch):
    dump = catalog.parse_dump_name("listenbrainz-dump-2651-20260904-000002-incremental/")
    return catalog.list_files(dump, fetch=fake_fetch)


def test_체크섬_파일은_따로_나열하지_않는다(files):
    assert all(not f.name.endswith((".md5", ".sha256")) for f in files)


def test_모든_파일에_체크섬이_묶인다(files):
    assert files
    for f in files:
        assert f.md5_url and f.md5_url.endswith(f"{f.name}.md5")
        assert f.sha256_url and f.sha256_url.endswith(f"{f.name}.sha256")
        assert f.has_checksum


def test_listens와_spark_덤프가_모두_있다(files):
    names = [f.name for f in files]
    assert any("listens-dump" in n for n in names)
    assert any("spark-dump" in n for n in names)


def test_URL이_디렉터리_아래로_만들어진다(files):
    for f in files:
        assert f.url.startswith(
            "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/incremental/"
        )
        assert f.url.endswith(f.name)


def test_키워드로_파일_하나를_고른다(files):
    picked = catalog.find_file(files, "listens-dump")
    assert picked.name.endswith(".tar.zst")


def test_걸리는_파일이_없으면_실패시킨다(files):
    with pytest.raises(CatalogError):
        catalog.find_file(files, "존재하지-않는-이름")


def test_여러_파일이_걸리면_실패시킨다(files):
    with pytest.raises(CatalogError):
        catalog.find_file(files, "listenbrainz-")


# ── 기타 ────────────────────────────────────────────────────

def test_상위_디렉터리_링크는_버린다():
    links = catalog.parse_links('<a href="../">..</a><a href="x/">x</a>')
    assert links == ["x/"]


def test_덤프_URL을_만든다():
    dump = catalog.parse_dump_name("listenbrainz-dump-2651-20260904-000002-incremental/")
    assert dump.url() == (
        "https://data.metabrainz.org/pub/musicbrainz/listenbrainz/"
        "incremental/listenbrainz-dump-2651-20260904-000002-incremental/"
    )
