import pytest

from flux.ingest import musicbrainz as mb
from flux.ingest.musicbrainz import MusicBrainzError

RELEASE = "20260902-002507"

SHA256SUMS = """\
75c013a56535bc1c4a69d02dc668a8593274ffcd9d42057348ec3198f64fc0ef  mbdump.tar.bz2
1f66a4b72c3bf985fd3e94fef4d8068243e8c7f2864163369977a7b53d29edf9  mbdump-derived.tar.bz2
aaaa1111bbbb2222cccc3333dddd4444eeee5555ffff6666aaaa7777bbbb8888  mbdump-edit.tar.bz2
"""

MD5SUMS = """\
28c74d6c15ff59873318645f34dafc81  mbdump.tar.bz2
1854612fa3df07d20307c02ba1b97831  mbdump-derived.tar.bz2
"""


def fake_fetch(pages):
    def _fetch(url):
        for suffix, body in pages.items():
            if url.endswith(suffix):
                return body
        raise OSError(f"준비되지 않은 URL: {url}")
    return _fetch


@pytest.fixture
def fetch():
    return fake_fetch({
        "/LATEST": RELEASE + "\n",
        "SHA256SUMS": SHA256SUMS,
        "MD5SUMS": MD5SUMS,
    })


# ── LATEST ──────────────────────────────────────────────────

def test_LATEST가_최신_릴리스를_알려준다(fetch):
    assert mb.latest_release(fetch=fetch) == RELEASE


def test_LATEST의_공백을_제거한다():
    assert mb.latest_release(fetch=lambda url: "  20260902-002507  \n") == RELEASE


@pytest.mark.parametrize("bad", ["", "   ", "../etc", "a/b"])
def test_이상한_LATEST는_거부한다(bad):
    """경로 조각이 들어오면 URL 조립이 엉뚱한 곳을 가리킨다."""
    with pytest.raises(MusicBrainzError):
        mb.latest_release(fetch=lambda url: bad)


# ── 체크섬 모음 파일 ────────────────────────────────────────

def test_모음_파일을_파일명_기준으로_읽는다():
    sums = mb.parse_sums(SHA256SUMS)
    assert sums["mbdump.tar.bz2"].startswith("75c013a5")
    assert len(sums) == 3


def test_별표_표시를_허용한다():
    """md5sum이 이진 모드로 만든 파일은 이름 앞에 *가 붙는다."""
    sums = mb.parse_sums("28c74d6c15ff59873318645f34dafc81 *mbdump.tar.bz2")
    assert sums["mbdump.tar.bz2"] == "28c74d6c15ff59873318645f34dafc81"


def test_해시를_소문자로_맞춘다():
    sums = mb.parse_sums("28C74D6C15FF59873318645F34DAFC81  x.tar.bz2")
    assert sums["x.tar.bz2"] == "28c74d6c15ff59873318645f34dafc81"


def test_빈_줄을_건너뛴다():
    assert len(mb.parse_sums("\n\n" + MD5SUMS + "\n")) == 2


def test_아무것도_못_읽으면_실패시킨다():
    with pytest.raises(MusicBrainzError):
        mb.parse_sums("이건 체크섬 파일이 아니다")


# ── 파일 목록 ───────────────────────────────────────────────

def test_필요한_것만_받는다(fetch):
    files = mb.list_files(fetch=fetch)
    names = [f.name for f in files]
    assert names == ["mbdump.tar.bz2", "mbdump-derived.tar.bz2"]


def test_15GB_편집_이력은_빼놓는다(fetch):
    """SHA256SUMS에는 있지만 목록에 넣지 않는다."""
    assert "mbdump-edit.tar.bz2" in mb.parse_sums(SHA256SUMS)
    assert all(f.name != "mbdump-edit.tar.bz2" for f in mb.list_files(fetch=fetch))


def test_두_해시가_모두_붙는다(fetch):
    core = mb.list_files(fetch=fetch)[0]
    assert core.md5 and core.sha256
    assert dict(core.checksums).keys() == {"md5", "sha256"}


def test_한쪽_모음_파일이_없어도_동작한다():
    """MD5SUMS가 없어도 SHA256SUMS만으로 검증한다."""
    files = mb.list_files(fetch=fake_fetch({
        "/LATEST": RELEASE,
        "SHA256SUMS": SHA256SUMS,
    }))
    core = files[0]
    assert core.sha256 and core.md5 is None
    assert dict(core.checksums).keys() == {"sha256"}


def test_체크섬_파일이_하나도_없으면_실패시킨다():
    with pytest.raises(MusicBrainzError):
        mb.list_files(fetch=fake_fetch({"/LATEST": RELEASE}))


def test_URL이_릴리스_아래로_만들어진다(fetch):
    for f in mb.list_files(fetch=fetch):
        assert f.url.endswith(f"/{RELEASE}/{f.name}")


def test_릴리스를_직접_지정할_수_있다(fetch):
    files = mb.list_files("20260829-002439", fetch=fetch)
    assert all("20260829-002439" in f.url for f in files)


# ── 릴리스 고정 ─────────────────────────────────────────────

NEWER = "20260905-002519"


def make_part(root, release, name="mbdump.tar.bz2.part", size=1):
    path = root / release / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


def test_받다_만_것이_없으면_LATEST를_따른다(tmp_path, fetch):
    chosen, warning = mb.resolve_release(tmp_path, fetch=fetch)
    assert chosen == RELEASE
    assert warning is None


def test_받다_만_것이_있으면_그것을_이어받는다(tmp_path):
    """LATEST가 교체돼도 진행 중인 것을 우선한다.

    실제로 겪은 문제다. 릴리스가 회전하면서 1.22GB를 받아 둔 조각이 버려지고
    새 디렉터리에 처음부터 받기 시작했다.
    """
    make_part(tmp_path, RELEASE)
    chosen, warning = mb.resolve_release(
        tmp_path, fetch=fake_fetch({"/LATEST": NEWER, "SHA256SUMS": SHA256SUMS})
    )
    assert chosen == RELEASE
    assert warning and NEWER in warning


def test_진행_중인_것이_최신이면_경고하지_않는다(tmp_path, fetch):
    make_part(tmp_path, RELEASE)
    chosen, warning = mb.resolve_release(tmp_path, fetch=fetch)
    assert chosen == RELEASE
    assert warning is None


def test_명시하면_그대로_쓴다(tmp_path):
    make_part(tmp_path, RELEASE)
    chosen, warning = mb.resolve_release(
        tmp_path, NEWER, fetch=lambda url: pytest.fail("서버를 부르면 안 된다")
    )
    assert chosen == NEWER
    assert warning is None


def test_빈_조각은_진행_중으로_보지_않는다(tmp_path, fetch):
    make_part(tmp_path, RELEASE, size=0)
    assert mb.in_progress_releases(tmp_path) == []


def test_완성된_파일만_있으면_진행_중이_아니다(tmp_path):
    path = tmp_path / RELEASE / "mbdump.tar.bz2"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"done")
    assert mb.in_progress_releases(tmp_path) == []


def test_여러_릴리스가_밀려_있으면_최신을_고른다(tmp_path):
    make_part(tmp_path, "20260829-002439")
    make_part(tmp_path, RELEASE)
    assert mb.in_progress_releases(tmp_path)[0] == RELEASE


def test_폴더가_없어도_안전하다(tmp_path):
    assert mb.in_progress_releases(tmp_path / "없는폴더") == []
