import io

import pytest

from flux.ingest import download
from flux.ingest.download import PART_SUFFIX, SizeMismatch


class FakeResponse(io.BytesIO):
    """urlopen이 돌려주는 객체를 흉내 낸다."""

    def __init__(self, data, headers=None, status=200):
        super().__init__(data)
        self.status = status
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


# ── 이어받기 ────────────────────────────────────────────────

def range_server(payload, *, supports_range=True, seen=None):
    """Range 요청을 처리하는 가짜 서버."""

    def _opener(url, timeout, headers=None):
        start = 0
        if headers and "Range" in headers:
            if seen is not None:
                seen.append(headers["Range"])
            if supports_range:
                start = int(headers["Range"].split("=")[1].split("-")[0])
        if start and supports_range:
            body = payload[start:]
            return FakeResponse(
                body,
                {"Content-Length": str(len(body)),
                 "Content-Range": f"bytes {start}-{len(payload) - 1}/{len(payload)}"},
                status=206,
            )
        return FakeResponse(payload, {"Content-Length": str(len(payload))}, status=200)

    return _opener


def test_조각이_있으면_Range를_보낸다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    part = dest.with_name(dest.name + PART_SUFFIX)
    part.write_bytes(payload[:1000])

    seen = []
    download.download("http://x/dump", dest, opener=range_server(payload, seen=seen))
    assert seen == ["bytes=1000-"]


def test_이어받아_완성한다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    part = dest.with_name(dest.name + PART_SUFFIX)
    part.write_bytes(payload[:1000])

    download.download("http://x/dump", dest, opener=range_server(payload))
    assert dest.read_bytes() == payload, "이어붙인 결과가 원본과 같아야 한다"


def test_남은_부분만_받는다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    part = dest.with_name(dest.name + PART_SUFFIX)
    part.write_bytes(payload[:1000])

    got = []
    download.download(
        "http://x/dump", dest, chunk_size=10 ** 9,
        progress=lambda r, t: got.append(r),
        opener=range_server(payload),
    )
    # 진행 보고는 이미 받은 1000바이트를 포함한 누적값이어야 한다
    assert got[-1] == len(payload)
    assert got[0] > 1000


def test_서버가_Range를_무시하면_처음부터_받는다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    part = dest.with_name(dest.name + PART_SUFFIX)
    part.write_bytes(b"garbage" * 100)

    download.download(
        "http://x/dump", dest, opener=range_server(payload, supports_range=False)
    )
    assert dest.read_bytes() == payload, "이전 조각이 섞이면 안 된다"


def test_resume이_꺼져_있으면_조각을_무시한다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    part = dest.with_name(dest.name + PART_SUFFIX)
    part.write_bytes(payload[:1000])

    seen = []
    download.download(
        "http://x/dump", dest, resume=False, opener=range_server(payload, seen=seen)
    )
    assert seen == [], "Range를 보내지 않아야 한다"
    assert dest.read_bytes() == payload


# ── 전체 크기 해석 ──────────────────────────────────────────

def test_206은_Content_Range에서_전체_크기를_읽는다():
    headers = {"Content-Length": "100", "Content-Range": "bytes 1000-1099/230992900"}
    assert download.parse_total_size(headers, 206) == 230992900


def test_200은_Content_Length가_전체_크기다():
    assert download.parse_total_size({"Content-Length": "230992900"}, 200) == 230992900


def test_크기를_알_수_없으면_None():
    assert download.parse_total_size({}, 200) is None
    assert download.parse_total_size({"Content-Length": "100"}, 206) is None


# ── 재시도 ──────────────────────────────────────────────────

def test_끊겨도_다시_시도해_완성한다(tmp_path, payload):
    dest = tmp_path / "dump.tar.zst"
    attempts = []

    def flaky(url, timeout, headers=None):
        attempts.append(headers)
        if len(attempts) == 1:
            # 첫 시도는 절반만 주고 끊긴다
            class Broken(FakeResponse):
                def __init__(self):
                    super().__init__(payload[: len(payload) // 2],
                                     {"Content-Length": str(len(payload))})
            return Broken()
        return range_server(payload)(url, timeout, headers)

    download.download_with_retry(
        "http://x/dump", dest, attempts=3, sleep=lambda s: None, opener=flaky
    )
    assert dest.read_bytes() == payload
    assert len(attempts) >= 2, "재시도가 일어나야 한다"


def test_계속_실패하면_포기한다(tmp_path):
    def always_broken(url, timeout, headers=None):
        raise ConnectionError("끊김")

    with pytest.raises(download.DownloadError) as exc:
        download.download_with_retry(
            "http://x/dump", tmp_path / "d.bin",
            attempts=3, sleep=lambda s: None, opener=always_broken,
        )
    assert "3번 시도" in str(exc.value)


def test_재시도_간격이_늘어난다(tmp_path):
    waits = []

    def always_broken(url, timeout, headers=None):
        raise ConnectionError("끊김")

    with pytest.raises(download.DownloadError):
        download.download_with_retry(
            "http://x/dump", tmp_path / "d.bin",
            attempts=4, backoff=2.0, sleep=waits.append, opener=always_broken,
        )
    assert waits == [1.0, 2.0, 4.0], "간격이 배로 늘어야 한다"
