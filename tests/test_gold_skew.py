"""스큐를 다루는 규칙. 소금 치기와 치우침 재기.

실제 실행은 Spark가 하고 여기서는 판단만 본다.
"""

import pytest

from flux.gold import skew

KEY = "1557e001-08ec-405d-a0cf-6d2625e90af5"


# ── 소금 치기 ───────────────────────────────────────────────

def test_소금을_치면_키가_갈라진다():
    assert skew.salted(KEY, 0) != skew.salted(KEY, 1)


def test_소금을_떼면_원래_키다():
    for salt in range(skew.SALT_BUCKETS):
        assert skew.unsalted(skew.salted(KEY, salt)) == KEY


def test_소금_없는_키는_그대로다():
    """1단계를 건너뛴 경로와 섞여도 안전해야 한다."""
    assert skew.unsalted(KEY) == KEY


def test_범위_밖_소금은_거부한다():
    """조용히 받으면 2단계에서 묶이지 않는 조각이 생긴다."""
    for bad in (-1, skew.SALT_BUCKETS, skew.SALT_BUCKETS + 1):
        with pytest.raises(ValueError):
            skew.salted(KEY, bad)


def test_구분자가_MBID에_안_들어간다():
    """UUID는 16진수와 붙임표뿐이다. 겹치면 떼어낼 때 모호해진다."""
    assert skew.SALT_SEPARATOR not in KEY


def test_소금_가짓수만큼_서로_다른_키가_나온다():
    keys = {skew.salted(KEY, s) for s in range(skew.SALT_BUCKETS)}
    assert len(keys) == skew.SALT_BUCKETS


# ── 실험 구성 ───────────────────────────────────────────────

def test_네_구성이다():
    """명세 6.4의 비교표가 넷이다."""
    assert len(skew.CONFIGS) == 4


def test_기준선은_AQE가_꺼져_있다():
    """Spark 3부터 AQE가 기본 켜짐이다. 끄지 않으면 '무처리 기준선'이
    이미 최적화된 상태라 salting 효과가 과소평가된다."""
    base = skew.CONFIGS[0]
    assert base.aqe is False and base.salting is False


def test_네_구성이_서로_다른_조합이다():
    combos = {(c.aqe, c.salting) for c in skew.CONFIGS}
    assert combos == {(False, False), (True, False), (False, True), (True, True)}


def test_AQE를_끄면_skewJoin도_꺼진다():
    """skewJoin은 AQE의 기능이다. 따로 켜 두면 설정을 읽는 사람이 헷갈린다."""
    conf = skew.config("aqe_off").spark_conf()
    assert conf["spark.sql.adaptive.enabled"] == "false"
    assert conf["spark.sql.adaptive.skewJoin.enabled"] == "false"


def test_AQE를_켜면_둘_다_켜진다():
    conf = skew.config("aqe_on").spark_conf()
    assert conf["spark.sql.adaptive.enabled"] == "true"
    assert conf["spark.sql.adaptive.skewJoin.enabled"] == "true"


def test_설정값이_문자열이다():
    """Spark 설정은 문자열만 받는다. True를 그대로 넣으면 터진다."""
    for c in skew.CONFIGS:
        for value in c.spark_conf().values():
            assert isinstance(value, str)


def test_구성을_이름으로_찾는다():
    assert skew.config("both").salting is True


def test_모르는_이름은_거부한다():
    with pytest.raises(KeyError):
        skew.config("aqe_maybe")


def test_구성마다_세_번_돌린다():
    """한 번만 재면 그날의 잡음과 효과가 구분되지 않는다."""
    assert skew.REPEATS == 3


# ── 치우침 재기 ─────────────────────────────────────────────

def test_고른_분포는_배수가_1이다():
    s = skew.skewness([100, 100, 100, 100])
    assert s["max_over_median"] == 1.0


def test_치우친_분포는_배수가_크다():
    s = skew.skewness([1, 1, 1, 1000])
    assert s["max_over_median"] == 1000.0


def test_전체와_키_수를_함께_센다():
    s = skew.skewness([1, 2, 3, 4])
    assert s["keys"] == 4 and s["total"] == 10


def test_가장_큰_키의_비중을_낸다():
    s = skew.skewness([10, 10, 80])
    assert s["top1_share"] == pytest.approx(0.8)


def test_순서가_달라도_같은_결과다():
    """입력 순서에 흔들리면 실행마다 다른 판정이 나온다."""
    assert (skew.skewness([5, 1, 9, 3])
            == skew.skewness([9, 3, 1, 5]))


def test_짝수_개수면_가운데_둘의_평균이다():
    assert skew.skewness([1, 3, 5, 7])["median"] == 4


def test_빈_입력은_거부한다():
    with pytest.raises(ValueError):
        skew.skewness([])
