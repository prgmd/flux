import io
import tarfile

import pytest

from flux.ingest import mbdump_reader as reader
from flux.ingest.mbdump_reader import MBDumpError


def make_dump(tmp_path, tables, *, name="mbdump.tar.bz2"):
    """테스트용 mbdump 형태의 tar.bz2를 만든다."""
    path = tmp_path / name
    with tarfile.open(path, "w:bz2") as tar:
        for table, body in tables.items():
            data = body.encode("utf-8")
            info = tarfile.TarInfo(f"mbdump/{table}")
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return path


ARTIST = "1\tabc\tThe Beatles\n2\tdef\tRadiohead\n3\tghi\tPortishead\n"
RELEASE = "1\tzzz\tAbbey Road\n2\tyyy\tOK Computer\n"


# ── 테이블 이름 판별 ─────────────────────────────────────────

@pytest.mark.parametrize("given, expected", [
    ("mbdump/artist", "artist"),
    ("mbdump/release_group", "release_group"),
    ("mbdump/", None),
    ("mbdump/sub/artist", None),
    ("COPYING", None),
    ("mbdump-derived/artist", None),
])
def test_테이블_이름을_가려낸다(given, expected):
    assert reader.table_name(given) == expected


# ── 행 수 세기 ──────────────────────────────────────────────

def test_줄_수와_바이트를_센다():
    rows, size = reader.count_rows(io.BytesIO(ARTIST.encode()))
    assert rows == 3
    assert size == len(ARTIST.encode())


def test_마지막_줄에_개행이_없어도_센다():
    """덤프 파일이 개행으로 끝나지 않는 경우가 있다."""
    rows, _ = reader.count_rows(io.BytesIO(b"a\nb\nc"))
    assert rows == 3


def test_빈_파일은_0행():
    rows, size = reader.count_rows(io.BytesIO(b""))
    assert rows == 0
    assert size == 0


def test_청크_크기가_결과를_바꾸지_않는다():
    data = ARTIST.encode()
    big = reader.count_rows(io.BytesIO(data), chunk_size=10 ** 9)
    tiny = reader.count_rows(io.BytesIO(data), chunk_size=3)
    assert big == tiny


# ── 덤프 훑기 ───────────────────────────────────────────────

def test_모든_테이블을_센다(tmp_path):
    dump = make_dump(tmp_path, {"artist": ARTIST, "release": RELEASE})
    stats = {s.name: s for s in reader.read_tables(dump)}
    assert stats["artist"].rows == 3
    assert stats["release"].rows == 2


def test_원하는_테이블만_센다(tmp_path):
    dump = make_dump(tmp_path, {"artist": ARTIST, "release": RELEASE})
    stats = list(reader.read_tables(dump, ["artist"]))
    assert [s.name for s in stats] == ["artist"]


def test_다_찾으면_끝까지_읽지_않는다(tmp_path):
    """7GB를 끝까지 읽지 않기 위한 조기 종료. 앞쪽 테이블만 요청하면 즉시 멈춘다."""
    dump = make_dump(tmp_path, {"artist": ARTIST, "b_table": RELEASE, "c_table": RELEASE})
    stats = list(reader.read_tables(dump, ["artist"]))
    assert len(stats) == 1


def test_없는_테이블을_요청하면_빈_결과(tmp_path):
    dump = make_dump(tmp_path, {"artist": ARTIST})
    assert list(reader.read_tables(dump, ["없는테이블"])) == []


def test_평균_행_크기를_계산한다(tmp_path):
    dump = make_dump(tmp_path, {"artist": ARTIST})
    stats = next(iter(reader.read_tables(dump)))
    assert stats.avg_row_bytes == round(len(ARTIST.encode()) / 3, 1)


def test_행이_0이면_평균이_0이다(tmp_path):
    dump = make_dump(tmp_path, {"empty": ""})
    stats = next(iter(reader.read_tables(dump)))
    assert stats.rows == 0
    assert stats.avg_row_bytes == 0.0


def test_표본을_뽑아도_전체_행_수는_맞다(tmp_path):
    """앞의 몇 줄을 미리 읽어도 합계가 어긋나면 안 된다."""
    dump = make_dump(tmp_path, {"artist": ARTIST})
    stats = next(iter(reader.read_tables(dump, sample_rows=2)))
    assert stats.rows == 3
    assert len(stats.sample) == 2
    assert stats.sample[0].startswith("1\t")


def test_표본이_행_수보다_많아도_안전하다(tmp_path):
    dump = make_dump(tmp_path, {"artist": ARTIST})
    stats = next(iter(reader.read_tables(dump, sample_rows=99)))
    assert stats.rows == 3
    assert len(stats.sample) == 3


def test_덤프가_없으면_실패시킨다(tmp_path):
    with pytest.raises(MBDumpError):
        list(reader.read_tables(tmp_path / "없는파일.tar.bz2"))


def test_테이블이_아닌_항목은_건너뛴다(tmp_path):
    path = tmp_path / "mbdump.tar.bz2"
    with tarfile.open(path, "w:bz2") as tar:
        for name, body in [("COPYING", "license"), ("mbdump/artist", ARTIST)]:
            data = body.encode()
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    assert [s.name for s in reader.read_tables(path)] == ["artist"]
