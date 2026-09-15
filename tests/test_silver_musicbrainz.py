"""MusicBrainz 덤프 테이블의 컬럼 순서와 값 해석.

덤프에는 헤더가 없으므로 **컬럼 순서가 곧 계약**이다. 틀려도 프로그램은 돌고
결과만 조용히 틀리므로, 선언과 개수 검사를 테스트로 못 박는다.

표본은 실제 덤프에서 본 줄을 그대로 쓴다.

날짜 조립과 boolean 해석은 Spark 잡이 자기 식으로 하므로 여기 없다.
쓰이지 않는 구현을 테스트하면 통과해도 아무것도 지켜 주지 않는다.
"""

import pytest

from flux.silver import musicbrainz as mb

# 실제 덤프에서 가져온 줄 (2026-09-05 릴리스)
ARTIST_LINE = (
    "371203\t49add228-eac5-4de8-836c-d75cde7369c3\tPete Moutso\tMoutso, Pete\t"
    "\\N\t\\N\t\\N\t\\N\t\\N\t\\N\t1\t\\N\t\\N\t\t0\t\\N\tf\t\\N\t\\N"
)
ISO_LINE = "1\tAF"


# ── 컬럼 선언 ───────────────────────────────────────────────

def test_dim_artist에_필요한_다섯_표가_선언돼_있다():
    """artist의 type, area, gender가 숫자 FK라 조회 표가 함께 필요하다."""
    assert set(mb.REQUIRED_TABLES) == {
        "artist", "area", "artist_type", "gender", "iso_3166_1"}


@pytest.mark.parametrize("table,count", [
    ("artist", 19), ("area", 14), ("artist_type", 6),
    ("gender", 6), ("iso_3166_1", 2),
])
def test_컬럼_개수가_실측과_맞다(table, count):
    assert len(mb.columns(table)) == count


def test_컬럼_이름이_겹치지_않는다():
    for table in mb.REQUIRED_TABLES:
        names = mb.columns(table)
        assert len(names) == len(set(names)), table


def test_artist의_앞_네_컬럼_순서():
    """이 넷이 틀리면 이름 자리에 다른 값이 들어간다."""
    assert mb.columns("artist")[:4] == ("id", "gid", "name", "sort_name")


def test_iso_3166_1은_area와_code_둘뿐이다():
    assert mb.columns("iso_3166_1") == ("area", "code")


def test_모르는_테이블은_거부한다():
    with pytest.raises(KeyError):
        mb.columns("없는테이블")


# ── 행 해석 ─────────────────────────────────────────────────

def test_실제_줄을_컬럼_이름에_맞춰_읽는다():
    row = mb.parse_row("artist", ARTIST_LINE)
    assert row["id"] == "371203"
    assert row["gid"] == "49add228-eac5-4de8-836c-d75cde7369c3"
    assert row["name"] == "Pete Moutso"
    assert row["sort_name"] == "Moutso, Pete"
    assert row["type"] == "1"
    assert row["ended"] == "f"


def test_두_컬럼_짜리도_읽는다():
    assert mb.parse_row("iso_3166_1", ISO_LINE) == {"area": "1", "code": "AF"}


def test_컬럼이_모자라면_거부한다():
    """조용히 넘어가면 컬럼이 한 칸씩 밀린 채 적재된다."""
    with pytest.raises(mb.ColumnCountError):
        mb.parse_row("artist", "1\t2\t3")


def test_컬럼이_남아도_거부한다():
    line = ARTIST_LINE + "\t추가"
    with pytest.raises(mb.ColumnCountError):
        mb.parse_row("artist", line)


def test_끝의_개행을_떼고_읽는다():
    assert mb.parse_row("iso_3166_1", ISO_LINE + "\n")["code"] == "AF"


# ── 없음의 표현 ─────────────────────────────────────────────

def test_NULL_표기는_None이_된다():
    row = mb.parse_row("artist", ARTIST_LINE)
    assert row["area"] is None
    assert row["gender"] is None


def test_빈_문자열은_값으로_남는다():
    """comment가 빈 문자열로 들어온다. 없는 것과 빈 것은 다르다."""
    assert mb.parse_row("artist", ARTIST_LINE)["comment"] == ""


def test_unnull은_NULL만_바꾼다():
    assert mb.unnull("\\N") is None
    assert mb.unnull("") == ""
    assert mb.unnull("N") == "N"
    assert mb.unnull("값") == "값"
