"""Silver 스키마 선언과, 규칙이 두 곳에서 갈리지 않게 막는 장치.

적재 잡(flux.silver.build)은 꼭대기에서 pyspark를 import하므로 호스트에서 읽히지
않는다. 그래서 그 잡이 **판단 모듈과 같은 규칙을 쓰는지**를 직접 검증할 수 없다.

대신 갈라질 수 있는 지점을 좁혀 둔다. 잡이 참조하는 값들이 빠짐없이 정의되어
있는지를 여기서 확인하면, 잡 쪽에서 조용히 하나를 빠뜨리는 일을 막을 수 있다.
"""

import pytest

from flux.silver import dedup, schema
from flux.silver.mbid import MbidSource
from flux.silver.quality import Reason


# ── 스키마 선언 ─────────────────────────────────────────────

def test_컬럼_이름이_겹치지_않는다():
    names = schema.column_names()
    assert len(names) == len(set(names))


def test_격리_컬럼_이름도_겹치지_않는다():
    names = schema.column_names(schema.QUARANTINE_COLUMNS)
    assert len(names) == len(set(names))


def test_파티션_컬럼이_스키마에_있다():
    assert schema.PARTITION_COLUMN in schema.column_names()


def test_격리_파티션_컬럼이_격리_스키마에_있다():
    assert schema.QUARANTINE_PARTITION_COLUMN in schema.column_names(
        schema.QUARANTINE_COLUMNS)


def test_파티션은_달_단위다():
    """실측에서 날짜 단위는 조각의 99.5%가 10만 행 미만이었다."""
    assert schema.PARTITION_COLUMN == "listened_month"


@pytest.mark.parametrize("name", [
    "listened_at", "user_id", "recording_msid", "mbid_source", "client_name",
])
def test_Gold_팩트가_요구하는_값이_있다(name):
    assert name in schema.column_names()


@pytest.mark.parametrize("name", ["_source_dump", "_record_id", "_ingested_at"])
def test_Bronze로_거슬러_올라갈_수_있다(name):
    """추적 가능성이 끊기면 집계가 이상할 때 원인을 못 찾는다."""
    assert name in schema.column_names()


def test_격리는_원본을_그대로_들고_간다():
    """판단이 틀렸을 때 되돌릴 수 있어야 한다."""
    names = schema.column_names(schema.QUARANTINE_COLUMNS)
    assert "raw_json" in names
    assert "reason" in names


@pytest.mark.parametrize("name", [
    "recording_mbid", "artist_mbids", "release_mbid",
    "track_name", "artist_name", "release_name", "duration_ms",
])
def test_없을_수_있는_값은_nullable이다(name):
    column = next(c for c in schema.COLUMNS if c.name == name)
    assert column.nullable is True


@pytest.mark.parametrize("name", [
    "listened_at", "user_id", "recording_msid", "mbid_source", "client_name",
])
def test_반드시_있어야_하는_값은_not_null이다(name):
    """이 다섯은 품질 규칙과 해석이 항상 채운다."""
    column = next(c for c in schema.COLUMNS if c.name == name)
    assert column.nullable is False


def test_모든_컬럼에_설명이_있다():
    assert all(c.doc.strip() for c in schema.COLUMNS)
    assert all(c.doc.strip() for c in schema.QUARANTINE_COLUMNS)


def test_아티스트_목록은_배열이다():
    column = next(c for c in schema.COLUMNS if c.name == "artist_mbids")
    assert column.type == "array<string>"


def test_모르는_타입은_거부한다():
    """Spark 없이 걸러야 컨테이너에 들어가기 전에 드러난다."""
    bad = (schema.Column("x", "이상한타입", True, "설명"),)
    with pytest.raises(ValueError):
        schema.validate_types(bad)


def test_선언된_타입이_전부_아는_것이다():
    schema.validate_types()
    schema.validate_types(schema.QUARANTINE_COLUMNS)


# ── 두 곳에서 갈리지 않게 ────────────────────────────────────

def test_모든_출처에_정렬_순위가_있다():
    """잡이 순위 컬럼을 만들 때 빠진 값이 있으면 그 행이 맨 뒤로 밀린다."""
    assert set(dedup.SOURCE_RANK) == set(MbidSource)


def test_정렬_순위가_서로_다르다():
    ranks = list(dedup.SOURCE_RANK.values())
    assert len(ranks) == len(set(ranks))


def test_정렬_명세의_컬럼이_작업_컬럼이거나_스키마_컬럼이다():
    """잡이 만들지 않는 컬럼으로 정렬하면 실행 시점에 터진다."""
    known = set(schema.column_names()) | set(schema.WORKING_COLUMNS)
    assert all(name in known for name, _ in dedup.ORDER)


def test_작업_컬럼은_최종_스키마에_남지_않는다():
    assert not set(schema.WORKING_COLUMNS) & set(schema.column_names())


def test_격리_사유가_서로_다르다():
    """사유가 겹치면 분포를 세는 의미가 없다."""
    reasons = [v for k, v in vars(Reason).items() if not k.startswith("_")]
    assert len(reasons) == len(set(reasons))
    assert len(reasons) == 5


def test_중복_제거_키가_스키마_컬럼이거나_해석된_값이다():
    """키 셋 중 둘은 스키마에 있고 listened_at은 타입만 바뀐다."""
    names = set(schema.column_names())
    assert {"user_name", "recording_msid", "listened_at"} <= names
    assert set(dedup.KEY) <= names
