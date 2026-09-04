import hashlib

import pytest

from flux.ingest import checksum
from flux.ingest.checksum import ChecksumError, ChecksumMismatch

# 실제 ListenBrainz 체크섬 파일은 해시만 들어 있고 개행도 없다.
REAL_MD5 = "28c74d6c15ff59873318645f34dafc81"
REAL_SHA = "1854612fa3df07d20307c02ba1b97831e4ebbf7b9e55a2db35df4f3dd0906dec"


@pytest.fixture
def sample(tmp_path):
    path = tmp_path / "sample.bin"
    path.write_bytes(b"flux" * 1000)
    return path


# ── 체크섬 파일 파싱 ──────────────────────────────────────────

def test_해시만_있는_형식을_읽는다():
    assert checksum.parse_checksum(REAL_MD5, "md5") == REAL_MD5
    assert checksum.parse_checksum(REAL_SHA, "sha256") == REAL_SHA


def test_파일명이_붙은_형식도_읽는다():
    assert checksum.parse_checksum(f"{REAL_MD5}  dump.tar.zst", "md5") == REAL_MD5


def test_앞뒤_공백과_개행을_무시한다():
    assert checksum.parse_checksum(f"  {REAL_SHA}\n", "sha256") == REAL_SHA


def test_대문자를_소문자로_맞춘다():
    assert checksum.parse_checksum(REAL_MD5.upper(), "md5") == REAL_MD5


@pytest.mark.parametrize("bad", ["", "   ", "\n"])
def test_빈_파일은_실패시킨다(bad):
    with pytest.raises(ChecksumError):
        checksum.parse_checksum(bad, "md5")


def test_길이가_다르면_실패시킨다():
    with pytest.raises(ChecksumError):
        checksum.parse_checksum(REAL_MD5[:-1], "md5")
    with pytest.raises(ChecksumError):
        checksum.parse_checksum(REAL_SHA, "md5")


def test_16진수가_아니면_실패시킨다():
    with pytest.raises(ChecksumError):
        checksum.parse_checksum("z" * 32, "md5")


def test_모르는_알고리즘은_실패시킨다():
    with pytest.raises(ChecksumError):
        checksum.parse_checksum(REAL_MD5, "crc32")


# ── 해시 계산 ────────────────────────────────────────────────

def test_알려진_값과_일치한다(sample):
    data = sample.read_bytes()
    assert checksum.hash_file(sample, "md5") == hashlib.md5(data).hexdigest()
    assert checksum.hash_file(sample, "sha256") == hashlib.sha256(data).hexdigest()


def test_청크_크기가_결과를_바꾸지_않는다(sample):
    """스트리밍으로 나눠 읽어도 한 번에 읽은 것과 같아야 한다."""
    once = checksum.hash_file(sample, "sha256", chunk_size=10 ** 9)
    tiny = checksum.hash_file(sample, "sha256", chunk_size=7)
    assert once == tiny


def test_빈_파일도_해시가_나온다(tmp_path):
    empty = tmp_path / "empty.bin"
    empty.write_bytes(b"")
    assert checksum.hash_file(empty, "md5") == hashlib.md5(b"").hexdigest()


# ── 대조 ────────────────────────────────────────────────────

def test_일치하면_해시를_돌려준다(sample):
    expected = checksum.hash_file(sample, "sha256")
    assert checksum.verify(sample, "sha256", expected) == expected


def test_불일치를_주입하면_실패시킨다(sample):
    """S1 게이트 항목. 검증 로직이 동작하는지는 실패 케이스로만 증명된다."""
    wrong = "0" * 64
    with pytest.raises(ChecksumMismatch) as exc:
        checksum.verify(sample, "sha256", wrong)
    assert exc.value.expected == wrong
    assert exc.value.actual == checksum.hash_file(sample, "sha256")


def test_한_바이트만_바뀌어도_잡아낸다(sample):
    """내용이 조금만 달라져도 해시는 완전히 달라진다."""
    expected = checksum.hash_file(sample, "sha256")
    data = bytearray(sample.read_bytes())
    data[0] ^= 0x01
    sample.write_bytes(bytes(data))
    with pytest.raises(ChecksumMismatch):
        checksum.verify(sample, "sha256", expected)


def test_불일치_메시지에_양쪽_값이_들어간다(sample):
    with pytest.raises(ChecksumMismatch) as exc:
        checksum.verify(sample, "md5", "1" * 32)
    message = str(exc.value)
    assert "1" * 32 in message
    assert checksum.hash_file(sample, "md5") in message


def test_두_알고리즘을_한_번에_대조한다(sample):
    results = checksum.verify_all(
        sample,
        [
            ("md5", checksum.hash_file(sample, "md5")),
            ("sha256", checksum.hash_file(sample, "sha256")),
        ],
    )
    assert set(results) == {"md5", "sha256"}


def test_하나라도_어긋나면_전체가_실패한다(sample):
    with pytest.raises(ChecksumMismatch):
        checksum.verify_all(
            sample,
            [
                ("md5", checksum.hash_file(sample, "md5")),
                ("sha256", "0" * 64),
            ],
        )


def test_대조할_것이_없으면_실패시킨다(sample):
    with pytest.raises(ChecksumError):
        checksum.verify_all(sample, [])
