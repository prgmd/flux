"""SCD Type 2 — 무엇이 바뀌면 새 버전인가.

이력이 잘못 쌓이면 **과거 청취가 엉뚱한 이름으로 조회된다.** 그리고 그 오류는
집계를 내야 드러나므로, 규칙 자체를 여기서 못 박는다.

실제 MERGE는 Spark가 하고 여기서는 판단만 본다.
"""

import pytest

from flux.gold import scd2

MBID = "1557e001-08ec-405d-a0cf-6d2625e90af5"
AT = "2026-09-15"
LATER = "2026-10-01"


def row(name="Prince", sort_name="Prince", country="US", type_="Person"):
    return {"artist_mbid": MBID, "name": name, "sort_name": sort_name,
            "country": country, "type": type_}


# ── 무엇을 추적하는가 ───────────────────────────────────────

def test_이름과_정렬이름만_추적한다():
    """나머지는 대부분 비어 있어 이력이 개명이 아니라 빈칸 채우기가 된다."""
    assert scd2.TRACKED_COLUMNS == ("name", "sort_name")


def test_자연키는_artist_mbid다():
    assert scd2.BUSINESS_KEY == "artist_mbid"


def test_추적_값을_선언_순서대로_뽑는다():
    assert scd2.tracked(row()) == ("Prince", "Prince")


def test_객체로_줘도_읽는다():
    """Spark Row는 사전처럼도 객체처럼도 읽힌다. 둘 다 받아야 한다."""
    class Fake:
        name = "Prince"
        sort_name = "Prince"
    assert scd2.tracked(Fake()) == ("Prince", "Prince")


def test_없는_컬럼은_None이다():
    assert scd2.tracked({}) == (None, None)


# ── 언제 새 버전인가 ────────────────────────────────────────

def test_이름이_바뀌면_새_버전이다():
    assert scd2.changed(row(name="Prince"), row(name="TAFKAP")) is True


def test_정렬이름만_바뀌어도_새_버전이다():
    assert scd2.changed(row(sort_name="Prince"),
                        row(sort_name="Prince, The")) is True


def test_아무것도_안_바뀌면_새_버전이_아니다():
    assert scd2.changed(row(), row()) is False


@pytest.mark.parametrize("field,value", [
    ("country", "GB"), ("type_", "Group"),
])
def test_추적하지_않는_컬럼은_이력을_만들지_않는다(field, value):
    """실측에서 country는 37.78%만 차 있다. 빈칸이 채워질 때마다 버전을
    만들면 이력의 대부분이 개명이 아닌 것이 된다."""
    assert scd2.changed(row(), row(**{field: value})) is False


def test_없던_이름이_생기면_새_버전이다():
    assert scd2.changed(row(name=None), row(name="Prince")) is True


def test_있던_이름이_사라져도_새_버전이다():
    assert scd2.changed(row(name="Prince"), row(name=None)) is True


def test_둘_다_없으면_같은_것이다():
    """파이썬의 != 가 None끼리를 같다고 보므로 그냥 된다.

    Spark는 반대라 NULL != NULL이 참도 거짓도 아니다. 그쪽은 eqNullSafe를
    써야 하고, 안 쓰면 이름 없는 아티스트가 매번 새 버전을 만든다.
    """
    assert scd2.changed(row(name=None), row(name=None)) is False


def test_빈_문자열과_없음은_다르다():
    assert scd2.changed(row(name=None), row(name="")) is True


# ── 이력을 닫고 여는 것 ─────────────────────────────────────

def test_새_이력은_열린_채로_시작한다():
    v = scd2.open_new(MBID, ("Prince", "Prince"), at=AT)
    assert v.is_current is True
    assert v.valid_from == AT
    assert v.valid_to == scd2.FAR_FUTURE


def test_닫으면_현재가_아니게_된다():
    v = scd2.close(scd2.open_new(MBID, ("Prince", "Prince"), at=AT), at=LATER)
    assert v.is_current is False
    assert v.valid_to == LATER


def test_닫아도_값과_시작_시각은_그대로다():
    """닫는 것은 유효 구간을 끊는 일이지 값을 고치는 일이 아니다."""
    opened = scd2.open_new(MBID, ("Prince", "Prince"), at=AT)
    closed = scd2.close(opened, at=LATER)
    assert closed.values == opened.values
    assert closed.valid_from == AT
    assert closed.key == MBID


def test_닫는_시각을_밖에서_받는다():
    """안에서 오늘을 읽으면 같은 데이터를 다시 처리할 때 결과가 달라진다."""
    opened = scd2.open_new(MBID, ("Prince",), at=AT)
    assert scd2.close(opened, at="2030-01-01").valid_to == "2030-01-01"
    assert scd2.close(opened, at="2027-01-01").valid_to == "2027-01-01"


def test_닫지_않은_이력은_먼_미래까지_유효하다():
    """NULL로 두면 기간 비교마다 'NULL인 경우'를 덧붙여야 하고,
    한 곳만 빠뜨리면 조용히 틀린다."""
    assert scd2.FAR_FUTURE == "9999-12-31"
    assert scd2.open_new(MBID, (), at=AT).valid_to == scd2.FAR_FUTURE


def test_이력은_고치지_않고_새_객체를_만든다():
    """Version이 frozen이라 제자리 수정이 막힌다. 이력은 덮어쓰면 안 된다."""
    v = scd2.open_new(MBID, ("Prince",), at=AT)
    with pytest.raises(Exception):
        v.is_current = False


# ── 개명 시나리오 ───────────────────────────────────────────

def test_개명하면_이력이_둘이_된다():
    """명세 게이트가 요구하는 시나리오다."""
    before = scd2.open_new(MBID, ("Prince", "Prince"), at=AT)
    after_change = scd2.changed(row(name="Prince"), row(name="TAFKAP"))
    assert after_change is True

    closed = scd2.close(before, at=LATER)
    opened = scd2.open_new(MBID, ("TAFKAP", "TAFKAP"), at=LATER)

    history = [closed, opened]
    assert sum(1 for v in history if v.is_current) == 1
    assert closed.valid_to == opened.valid_from


def test_유효_구간이_겹치지_않는다():
    """명세 게이트의 SCD2 불변식이다. 닫은 시각과 연 시각이 같아야
    빈틈도 겹침도 없다."""
    closed = scd2.close(scd2.open_new(MBID, ("A",), at=AT), at=LATER)
    opened = scd2.open_new(MBID, ("B",), at=LATER)
    assert closed.valid_to == opened.valid_from
    assert closed.valid_from < closed.valid_to
