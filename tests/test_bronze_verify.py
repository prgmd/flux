import pytest

from flux.bronze import verify


# ── 불변식 ──────────────────────────────────────────────────

def test_합이_맞으면_통과():
    r = verify.reconcile(100, 95, 5)
    assert r.ok
    assert r.missing == 0
    assert "= Bronze 95 + 격리 5" in r.explain()


def test_줄이_사라지면_잡아낸다():
    r = verify.reconcile(100, 90, 5)
    assert not r.ok
    assert r.missing == 5
    assert "사라졌다" in r.explain()


def test_줄이_늘어나면_잡아낸다():
    """멱등성이 깨져 중복 적재된 경우다."""
    r = verify.reconcile(100, 190, 5)
    assert not r.ok
    assert r.missing == -95
    assert "늘었다" in r.explain()
    assert "중복 적재" in r.explain()


def test_행_수만_맞아서는_통과하지_않는다():
    """유효한 줄이 격리로 새는 것도 잡아야 한다.

    Bronze만 세면 95든 90이든 '데이터가 있다'로 보인다.
    격리까지 더해 원본과 맞춰야 어긋남이 드러난다.
    """
    새는_경우 = verify.reconcile(100, 90, 5)
    정상 = verify.reconcile(100, 90, 10)
    assert not 새는_경우.ok
    assert 정상.ok


def test_전부_격리돼도_합은_맞는다():
    """합만 맞으면 통과한다. 품질 판단은 격리 비율이 따로 말해 준다."""
    r = verify.reconcile(100, 0, 100)
    assert r.ok
    assert r.quarantine_ratio == 1.0


def test_빈_적재도_통과():
    r = verify.reconcile(0, 0, 0)
    assert r.ok
    assert r.quarantine_ratio == 0.0


@pytest.mark.parametrize("args", [(-1, 0, 0), (10, -1, 0), (10, 0, -1)])
def test_음수는_거부한다(args):
    with pytest.raises(ValueError):
        verify.reconcile(*args)


def test_격리_비율을_계산한다():
    assert verify.reconcile(1000, 999, 1).quarantine_ratio == 0.001


# ── 줄 세기 ─────────────────────────────────────────────────

def test_줄_수를_센다(tmp_path):
    path = tmp_path / "a.jsonl"
    path.write_bytes(b'{"a":1}\n{"b":2}\n{"c":3}\n')
    assert verify.count_lines(path) == 3


def test_마지막_줄에_개행이_없어도_센다(tmp_path):
    path = tmp_path / "a.jsonl"
    path.write_bytes(b'{"a":1}\n{"b":2}')
    assert verify.count_lines(path) == 2


def test_빈_파일은_0줄(tmp_path):
    path = tmp_path / "a.jsonl"
    path.write_bytes(b"")
    assert verify.count_lines(path) == 0


def test_빈_줄도_한_줄로_센다(tmp_path):
    """격리 대상이 되더라도 원본에 존재하는 줄이므로 세야 등식이 맞는다."""
    path = tmp_path / "a.jsonl"
    path.write_bytes(b'{"a":1}\n\n{"b":2}\n')
    assert verify.count_lines(path) == 3


def test_청크_크기가_결과를_바꾸지_않는다(tmp_path):
    path = tmp_path / "a.jsonl"
    path.write_bytes(b'{"a":1}\n{"b":2}\n{"c":3}')
    assert verify.count_lines(path, chunk_size=3) == verify.count_lines(path, chunk_size=10 ** 9)
