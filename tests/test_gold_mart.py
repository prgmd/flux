"""급상승 마트의 순위 규칙. Spark 없이 돈다."""

from flux.gold import mart


def test_비율이_큰_순서다():
    this = {"a": 60, "b": 100, "c": 30}
    previous = {"a": 20, "b": 50, "c": 20}
    assert [r[0] for r in mart.rising(this, previous)] == ["a", "b", "c"]


def test_지난주가_하한보다_적으면_빠진다():
    """지난주 1번, 이번 주 30번이면 30배지만 급상승이 아니다."""
    this = {"tiny": 30, "real": 40}
    previous = {"tiny": 1, "real": 20}
    assert [r[0] for r in mart.rising(this, previous)] == ["real"]


def test_하한은_지난주_20이다():
    assert mart.MIN_PREVIOUS_LISTENS == 20
    this = {"edge": 40, "below": 40}
    previous = {"edge": 20, "below": 19}
    assert [r[0] for r in mart.rising(this, previous)] == ["edge"]


def test_지난주에_없던_아티스트는_비율을_낼_수_없어_빠진다():
    assert mart.rising({"new": 500}, {}) == []


def test_비율이_같으면_이번_주가_많은_쪽이다():
    this = {"small": 40, "big": 80}
    previous = {"small": 20, "big": 40}
    assert [r[0] for r in mart.rising(this, previous)] == ["big", "small"]


def test_전부_같으면_MBID_순이다():
    this = {"b": 40, "a": 40}
    previous = {"b": 20, "a": 20}
    assert [r[0] for r in mart.rising(this, previous)] == ["a", "b"]


def test_상위_N개만_남긴다():
    this = {f"a{i:02}": 100 + i for i in range(30)}
    previous = {artist: 20 for artist in this}
    assert len(mart.rising(this, previous)) == mart.RISING_TOP
