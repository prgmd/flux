"""팩트 한 줄이 무엇인가, 그리고 차원에 없는 아티스트를 어떻게 다루는가.

실제 적재는 Spark가 하고 여기서는 선언과 판단만 본다.
"""

import pytest

from flux.gold import fact, scd2
from flux.silver import schema as silver_schema

MBID = "1557e001-08ec-405d-a0cf-6d2625e90af5"


# ── 한 줄이 무엇인가 ────────────────────────────────────────

def test_청취_식별자는_한_컬럼이_아니라_쌍이다():
    """_record_id는 monotonically_increasing_id()로 매겨 **잡 하나 안에서만**
    유일하다. 덤프마다 별개 잡으로 실었으므로 번호가 덤프를 건너 겹친다.

    실측에서 151,192,624행에 서로 다른 _record_id가 9,487,346개뿐이었다.
    이걸 혼자 식별자로 쓰면 부풀림이 16배로 나온다."""
    assert fact.LISTEN_KEY == ("_source_dump", "_record_id")
    for name in fact.LISTEN_KEY:
        assert name in fact.column_names()


def test_grain_키는_청취_키에_아티스트를_더한_것이다():
    """팩트 한 줄은 청취-아티스트 쌍이다. 청취 키만으로는 유일하지 않고,
    아티스트 수만큼 반복되는 것이 정상이다."""
    assert fact.GRAIN_KEY == ("_source_dump", "_record_id", "artist_mbid")


def test_청취_식별자는_비어_있으면_안_된다():
    """NULL이면 그 줄의 청취를 셀 수 없다."""
    for name in fact.LISTEN_KEY:
        assert next(c for c in fact.COLUMNS if c.name == name).nullable is False


def test_아티스트는_한_줄에_하나다():
    """Silver는 배열이지만 팩트는 펼친 결과다."""
    artist = next(c for c in fact.COLUMNS if c.name == "artist_mbid")
    assert artist.type == "string"
    assert artist.nullable is True


def test_아티스트가_없어도_되는_줄이다():
    """실측에서 60.32%가 아티스트 MBID가 없다. nullable이 아니면
    그 행들이 적재에서 터진다."""
    assert next(c for c in fact.COLUMNS if c.name == "artist_mbid").nullable


def test_달_단위로_파티션한다():
    """명세 5.3은 listened_date였다. Silver와 같은 listened_at을 쓰므로
    날짜로 나누면 조각이 7,877개가 된다."""
    assert fact.PARTITION_COLUMN == "listened_month"
    assert fact.PARTITION_COLUMN in fact.column_names()


def test_Silver와_같은_파티션_기준이다():
    """두 층이 다른 기준으로 나뉘면 층을 잇는 잡이 매번 다시 섞어야 한다."""
    assert fact.PARTITION_COLUMN == silver_schema.PARTITION_COLUMN


def test_출처를_팩트까지_들고_온다():
    """mbid_source가 이 프로젝트의 데이터 품질 핵심이다. 하류에서
    신뢰도별로 거를 수 있어야 한다."""
    assert "mbid_source" in fact.column_names()


def test_타입_이름이_전부_아는_것이다():
    silver_schema.validate_types(fact.COLUMNS)


def test_컬럼_이름이_겹치지_않는다():
    names = fact.column_names()
    assert len(names) == len(set(names))


# ── 차원에 없는 아티스트 ────────────────────────────────────

def test_자리를_만들어_준다():
    assert fact.placeholder(MBID).artist_mbid == MBID


def test_이름을_NULL로_두지_않는다():
    """NULL이면 '이름이 없는 아티스트'와 '차원에 없는 아티스트'가
    집계에서 똑같이 빈칸으로 나온다. 모르는 이유가 보여야 고칠 수 있다."""
    assert fact.placeholder(MBID).name == fact.PLACEHOLDER_NAME
    assert fact.placeholder(MBID).name is not None


def test_먼_과거부터_유효하다():
    """어떤 청취든 덮어야 한다. 청취 기록은 2005년부터 있다."""
    assert fact.placeholder(MBID).valid_from < "2005-01-01"


def test_빈_MBID로는_자리를_못_만든다():
    """빈 문자열이 차원에 들어가면 그 자리가 모든 결측을 빨아들인다."""
    with pytest.raises(ValueError):
        fact.placeholder("")


def test_아직_안_채워진_자리를_알아본다():
    """다음 릴리스에 진짜 이름이 들어오면 SCD2가 덮는다.
    그때까지 몇 개 남았는지 세는 데 쓴다."""
    assert fact.is_placeholder(fact.PLACEHOLDER_NAME) is True
    assert fact.is_placeholder("Prince") is False
    assert fact.is_placeholder(None) is False


# ── 시점 조인의 판단 ────────────────────────────────────────

OPENED = "2020-01-01"
CHANGED = "2024-06-01"


def closed():
    return scd2.close(scd2.open_new(MBID, ("옛 이름",), at=OPENED), at=CHANGED)


def current():
    return scd2.open_new(MBID, ("새 이름",), at=CHANGED)


def test_구간_안의_시점을_덮는다():
    assert scd2.covers(closed(), "2022-01-01") is True


def test_구간_밖의_시점은_안_덮는다():
    assert scd2.covers(closed(), "2025-01-01") is False


def test_구간보다_이른_시점도_안_덮는다():
    assert scd2.covers(closed(), "2019-12-31") is False


def test_시작_시각은_포함한다():
    assert scd2.covers(closed(), OPENED) is True


def test_끝_시각은_제외한다():
    """양끝을 다 포함하면 닫힌 줄과 열린 줄이 같은 시각에 둘 다 걸린다."""
    assert scd2.covers(closed(), CHANGED) is False


def test_경계에서_정확히_한_줄만_걸린다():
    """SCD2 불변식이 조인에서도 성립해야 한다. 여기가 깨지면
    그 시각의 집계만 두 배가 되고, 찾기 어렵다."""
    history = [closed(), current()]
    for at in (OPENED, "2022-01-01", CHANGED, "2030-01-01"):
        assert sum(1 for v in history if scd2.covers(v, at)) == 1


def test_열린_줄은_먼_미래까지_덮는다():
    assert scd2.covers(current(), "9999-12-30") is True


def test_먼_미래_자체는_안_덮는다():
    """valid_to가 9999-12-31이고 끝은 제외이므로 그 하루만 비어 있다.
    실제 청취가 그 날짜일 수 없으므로 문제가 되지 않는다."""
    assert scd2.covers(current(), scd2.FAR_FUTURE) is False


# ── 청취 하나가 몇 줄이 되는가 ──────────────────────────────

OTHER = "8538e728-ca0b-4321-b7e5-cff6565dd4c0"


def test_아티스트가_없으면_한_줄을_남긴다():
    """explode는 이 줄을 없앤다. 실측에서 60.32%, 9,119만 행이 여기 해당한다.
    사라져도 에러가 나지 않아 청취 수 집계가 조용히 40%짜리가 된다."""
    for empty in (None, (), []):
        assert fact.artists_of(empty) == (None,)
        assert fact.fan_out(empty) == 1


def test_아티스트가_하나면_한_줄이다():
    assert fact.artists_of([MBID]) == (MBID,)
    assert fact.fan_out([MBID]) == 1


def test_아티스트가_둘이면_두_줄이다():
    """실측에서 2명 이상이 0.31%뿐이라 펼침 비용이 +0.42%로 끝난다."""
    assert fact.artists_of([MBID, OTHER]) == (MBID, OTHER)
    assert fact.fan_out([MBID, OTHER]) == 2


def test_같은_아티스트가_두_번이면_한_줄이다():
    """Silver는 형식만 걸러내고 중복은 남긴다. 그대로 펼치면 GRAIN_KEY가
    완전히 같은 줄이 둘 생겨 불변식이 깨지고, 그 아티스트의 청취 수가
    두 배로 세어진다."""
    assert fact.artists_of([MBID, MBID]) == (MBID,)
    assert fact.fan_out([MBID, MBID]) == 1


def test_중복을_지워도_다른_아티스트는_남는다():
    assert fact.artists_of([MBID, OTHER, MBID]) == (MBID, OTHER)


def test_처음_나온_순서를_지킨다():
    """Spark의 array_distinct가 그렇게 동작한다. 두 구현이 다른 순서를 주면
    이 함수로 확인한 것이 그쪽 결과와 다르다."""
    assert fact.artists_of([OTHER, MBID, OTHER]) == (OTHER, MBID)


def test_grain이_유일해지는_것이_펼치기의_조건이다():
    """한 청취가 만드는 줄들의 artist_mbid는 서로 달라야 한다.
    GRAIN_KEY의 앞 두 값은 그 청취 안에서 같으므로, 유일성은 여기서만 온다."""
    for mbids in ([], [MBID], [MBID, MBID], [MBID, OTHER, MBID]):
        artists = fact.artists_of(mbids)
        assert len(artists) == len(set(artists))


def test_적재_잡이_array_distinct를_거쳐_펼친다():
    """flux.gold.fact_listens는 pyspark를 부르므로 호스트에서 import되지 않는다.
    원문으로 확인한다. explode_outer만 쓰고 array_distinct를 빠뜨리면
    위 규칙이 Spark 쪽에서만 깨진다."""
    import re
    from pathlib import Path

    import flux.gold

    source = (Path(flux.gold.__file__).parent / "fact_listens.py").read_text(
        encoding="utf-8")
    # 줄바꿈과 들여쓰기에 흔들리지 않게 공백을 지우고 본다.
    squeezed = re.sub(r"\s+", "", source)
    assert "explode_outer(F.array_distinct(" in squeezed
    assert "F.explode(F.col" not in squeezed
