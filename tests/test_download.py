import io

import pytest

from flux.ingest import download
from flux.ingest.download import PART_SUFFIX, SizeMismatch


class FakeResponse(io.BytesIO):
    """urlopen이 돌려주는 객체를 흉내 낸다."""

    def __init__(self, data, headers=None):
        super().__init__(data)
        self.headers = headers if headers is not None else {"Content-Length": str(len(data))}

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


def opener_for(data, headers=None, *, calls=None):
    def _opener(url, timeout, extra=None):
        if calls is not None:
            calls.append(url)
        return FakeResponse(data, headers)
    return _opener


@pytest.fixture
def payload():
    return b"flux-dump-" * 5000


def test_파일로_받는다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    result = download.download("http://x/dump", dest, opener=opener_for(payload))
    assert result == dest
    assert dest.read_bytes() == payload


def test_받는_동안에는_part_파일에_쓴다(tmp_path, payload):
    """중단된 다운로드가 완성본으로 오인되면 안 된다."""
    dest = tmp_path / "dump.tar.zst"
    seen = []

    def watching_opener(url, timeout, extra=None):
        seen.append(sorted(p.name for p in tmp_path.iterdir()))
        return FakeResponse(payload)

    download.download("http://x/dump", dest, opener=watching_opener)
    # 완료 후에는 part가 남지 않는다
    assert [p.name for p in tmp_path.iterdir()] == ["dump.tar.zst"]


def test_중간에_끊기면_완성본을_남기지_않는다(tmp_path, payload):
    class Broken(FakeResponse):
        def read(self, size=-1):
            raise ConnectionError("연결이 끊겼다")

    def broken_opener(url, timeout, extra=None):
        return Broken(payload)

    dest = tmp_path / "dump.tar.zst"
    with pytest.raises(ConnectionError):
        download.download("http://x/dump", dest, opener=broken_opener)

    assert not dest.exists(), "완성본이 만들어지면 안 된다"
    assert (dest.with_name(dest.name + PART_SUFFIX)).exists(), "받다 만 조각은 남아야 이어받을 수 있다"


def test_크기가_다르면_실패시키고_조각을_지운다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    headers = {"Content-Length": str(len(payload) + 100)}
    with pytest.raises(SizeMismatch):
        download.download("http://x/dump", dest, opener=opener_for(payload, headers))

    assert not dest.exists()
    assert not (dest.with_name(dest.name + PART_SUFFIX)).exists()


def test_크기를_모르면_그대로_받는다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    download.download("http://x/dump", dest, opener=opener_for(payload, {}))
    assert dest.read_bytes() == payload


def test_이미_있으면_다시_받지_않는다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    dest.write_bytes(b"already-here")
    calls = []
    download.download("http://x/dump", dest, opener=opener_for(payload, calls=calls))
    assert calls == [], "서버를 부르지 않아야 한다"
    assert dest.read_bytes() == b"already-here"


def test_overwrite면_다시_받는다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    dest.write_bytes(b"already-here")
    download.download("http://x/dump", dest, overwrite=True, opener=opener_for(payload))
    assert dest.read_bytes() == payload


def test_상위_디렉터리를_만든다(tmp_path, payload):
    dest = tmp_path / "a" / "b" / "dump.tar.zst"
    download.download("http://x/dump", dest, opener=opener_for(payload))
    assert dest.exists()


def test_진행_상황을_보고한다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    seen = []
    download.download(
        "http://x/dump", dest, chunk_size=1024,
        progress=lambda got, total: seen.append((got, total)),
        opener=opener_for(payload),
    )
    assert seen, "진행 보고가 없다"
    assert seen[-1][0] == len(payload)
    assert all(a[0] <= b[0] for a, b in zip(seen, seen[1:])), "받은 양은 줄지 않는다"


def test_진행_표시_문자열(tmp_path):
    assert "50%" in download.format_progress(50, 100)
    assert "MB" in download.format_progress(1024 * 1024, None)
