"""Gold 잡이 판단 모듈과 같은 규칙을 쓰는가.

셋을 본다.

    explode_artists   ↔  fact.artists_of   청취 하나가 몇 줄이 되는가
    merge             ↔  scd2.stage        무엇을 몇 줄로 MERGE에 보내는가
    with_artist_name  ↔  scd2.covers       어느 이력 줄이 이 시점을 덮는가

이 셋이 이 프로젝트에서 실제로 물렸던 자리다. `_record_id` 부풀림 16배,
`explode`로 9,119만 행 소실, 2단계 MERGE의 반쪽 상태.
"""

from datetime import datetime, timezone

import pytest

# -m 필터는 수집이 끝난 뒤에 걸린다. 그래서 여기서 먼저 막아야 한다.
# 이 아래 import가 pyspark를 부르므로, 없는 환경에서는 수집 자체가 실패한다.
pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from flux.gold import dim_artist, fact, fact_listens, scd2

pytestmark = pytest.mark.spark

MBID_A = "1557e001-08ec-405d-a0cf-6d2625e90af5"
MBID_B = "8538e728-ca0b-4321-b7e5-cff6565dd4c0"
MBID_C = "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d"

BUILT_AT = datetime(2026, 9, 15, 0, 0, tzinfo=timezone.utc)


# ── 펼치기 ──────────────────────────────────────────────────

ARTISTS_SCHEMA = "listen_id long, artist_mbids array<string>"

EXPLODE_CASES = [
    ("아티스트 없음", None),
    ("빈 배열", []),
    ("하나", [MBID_A]),
    ("둘", [MBID_A, MBID_B]),
    ("같은 것 둘", [MBID_A, MBID_A]),
    ("중복 섞임", [MBID_A, MBID_B, MBID_A]),
]


@pytest.mark.parametrize("label,mbids",
                         EXPLODE_CASES, ids=[c[0] for c in EXPLODE_CASES])
def test_펼친_결과가_fact_artists_of와_같다(spark, label, mbids):
    """explode_outer와 array_distinct를 둘 다 확인한다.

    explode를 쓰면 앞의 두 경우가 사라지고, array_distinct를 빼면
    뒤의 두 경우가 GRAIN_KEY를 겹치게 만든다.
    """
    df = spark.createDataFrame([(1, mbids)], ARTISTS_SCHEMA)
    rows = fact_listens.explode_artists(df).collect()

    assert tuple(row["artist_mbid"] for row in rows) == fact.artists_of(mbids)
    assert len(rows) == fact.fan_out(mbids)


def test_아티스트가_없는_청취가_사라지지_않는다(spark):
    """실측에서 60.32%, 9,119만 행이 여기 해당한다. explode를 쓰면
    에러 없이 사라지고 청취 수 집계가 40%짜리가 된다."""
    rows = [(1, None), (2, []), (3, [MBID_A])]
    df = spark.createDataFrame(rows, ARTISTS_SCHEMA)
    exploded = fact_listens.explode_artists(df)

    assert exploded.select("listen_id").distinct().count() == 3


def test_배열_안_중복이_grain을_겹치게_하지_않는다(spark):
    """같은 MBID가 두 번 든 청취를 그대로 펼치면 GRAIN_KEY가 완전히 같은
    줄이 둘 생기고, 그 아티스트의 청취 수가 두 배로 세어진다."""
    df = spark.createDataFrame([(1, [MBID_A, MBID_A, MBID_B])], ARTISTS_SCHEMA)
    exploded = fact_listens.explode_artists(df)

    assert exploded.count() == 2
    assert exploded.select("listen_id", "artist_mbid").distinct().count() == 2


def test_배열_안_중복이_몇_개인지_센다(spark):
    """실측에 이 숫자가 없다. measure_fact_grain.py는 배열이 몇 개인지만 셌다."""
    rows = [(1, [MBID_A, MBID_A]), (2, [MBID_A, MBID_B]), (3, None)]
    df = spark.createDataFrame(rows, ARTISTS_SCHEMA)
    total = df.agg(
        fact_listens.collapsed_by_distinct(df).alias("n")).first()["n"]
    assert total == 1


# ── SCD Type 2 ──────────────────────────────────────────────

DIM_INPUT_SCHEMA = (
    "artist_mbid string, name string, sort_name string, "
    "country string, area_name string, type string, gender string, "
    "begin_date string, end_date string, ended boolean, comment string, "
    "mb_artist_id string"
)


def _artist(mbid, name, *, sort_name=None):
    return (mbid, name, sort_name or name,
            None, None, None, None, None, None, None, None, "1")


def _dim_path(warehouse):
    return warehouse("gold", scd2.TABLE_NAME)


def _create(spark, warehouse, artists, *, at, release="r1"):
    path = _dim_path(warehouse)
    silver = spark.createDataFrame(artists, DIM_INPUT_SCHEMA)
    dim_artist.create(dim_artist.prepare(silver, at=at, release=release), path)
    return path


def test_처음_만들면_모두_첫_버전이다(spark, warehouse):
    path = _create(spark, warehouse,
                   [_artist(MBID_A, "가수"), _artist(MBID_B, "밴드")],
                   at="2026-09-15")

    dim = spark.read.format("delta").load(path)
    assert dim.count() == 2
    assert dim.filter(dim[scd2.IS_CURRENT]).count() == 2
    assert dim_artist.invariants(spark, path) == {
        "keys_without_exactly_one_current": 0,
        "keys_with_no_current": 0,
        "rows_with_invalid_range": 0,
    }


def test_개명하면_두_줄이_되고_불변식이_지켜진다(spark, warehouse):
    """scd2.stage가 말하는 것 — 값이 바뀐 아티스트는 두 줄로 나간다.
    하나는 닫으러, 하나는 열러."""
    path = _create(spark, warehouse, [_artist(MBID_A, "옛 이름")],
                   at="2026-09-15")

    silver = spark.createDataFrame([_artist(MBID_A, "새 이름")],
                                   DIM_INPUT_SCHEMA)
    prepared = dim_artist.prepare(silver, at="2026-09-16", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-16")

    dim = spark.read.format("delta").load(path)
    assert dim.count() == 2
    assert dim_artist.invariants(spark, path) == {
        "keys_without_exactly_one_current": 0,
        "keys_with_no_current": 0,
        "rows_with_invalid_range": 0,
    }

    closed = dim.filter(~dim[scd2.IS_CURRENT]).first()
    current = dim.filter(dim[scd2.IS_CURRENT]).first()
    assert closed["name"] == "옛 이름"
    assert closed[scd2.VALID_TO] == "2026-09-16"
    assert current["name"] == "새 이름"
    assert current[scd2.VALID_FROM] == "2026-09-16"
    assert current[scd2.VALID_TO] == scd2.FAR_FUTURE


def test_안_바뀐_아티스트는_줄이_늘지_않는다(spark, warehouse):
    """추적 대상은 name과 sort_name뿐이다. 나머지가 채워지는 것을 '바뀐'으로
    세면 이력의 대부분이 개명이 아니라 편집자가 빈칸을 채운 기록이 된다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "가수")], at="2026-09-15")

    same = list(_artist(MBID_A, "가수"))
    same[3] = "KR"  # country만 채운다. 추적 대상이 아니다
    prepared = dim_artist.prepare(
        spark.createDataFrame([tuple(same)], DIM_INPUT_SCHEMA),
        at="2026-09-16", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-16")

    assert spark.read.format("delta").load(path).count() == 1


def test_새_아티스트는_한_줄로_들어온다(spark, warehouse):
    path = _create(spark, warehouse, [_artist(MBID_A, "가수")], at="2026-09-15")

    prepared = dim_artist.prepare(
        spark.createDataFrame(
            [_artist(MBID_A, "가수"), _artist(MBID_B, "신인")],
            DIM_INPUT_SCHEMA),
        at="2026-09-16", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-16")

    dim = spark.read.format("delta").load(path)
    assert dim.count() == 2
    assert dim.filter(dim[scd2.IS_CURRENT]).count() == 2


def test_보낸_줄_수가_scd2_stage와_같다(spark, warehouse):
    """부풀리기 규칙 자체를 대조한다. 개명 하나와 신규 하나면
    닫기 1 + 열기 1 + 신규 1 = 3줄이다."""
    path = _create(spark, warehouse,
                   [_artist(MBID_A, "옛 이름"), _artist(MBID_B, "그대로")],
                   at="2026-09-15")

    incoming = [_artist(MBID_A, "새 이름"), _artist(MBID_B, "그대로"),
                _artist(MBID_C, "신인")]
    prepared = dim_artist.prepare(
        spark.createDataFrame(incoming, DIM_INPUT_SCHEMA),
        at="2026-09-16", release="r2")

    def as_tracked(rows):
        return {r[0]: {"name": r[1], "sort_name": r[2]} for r in rows}

    expected = scd2.stage(
        as_tracked([_artist(MBID_A, "옛 이름"), _artist(MBID_B, "그대로")]),
        as_tracked(incoming))

    staged = dim_artist._staged(spark, path, prepared)
    assert staged.count() == len(expected)

    keys = sorted(
        (row[scd2.MERGE_KEY] or "") for row in staged.collect())
    assert keys == sorted((key or "") for key, _ in expected)


def test_같은_날짜로_다시_실으면_거부한다(spark, warehouse):
    """닫으면 valid_from == valid_to가 되어 잡이 자기 게이트를 깬다.
    쓰기 전에 막는다 — 쓴 뒤에 잡으면 망가진 차원을 되돌려야 한다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "옛 이름")],
                   at="2026-09-15")

    prepared = dim_artist.prepare(
        spark.createDataFrame([_artist(MBID_A, "새 이름")], DIM_INPUT_SCHEMA),
        at="2026-09-15", release="r2")

    assert dim_artist.unclosable(spark, path, prepared, at="2026-09-15") == 1
    with pytest.raises(dim_artist.SameDayReload):
        dim_artist.merge(spark, path, prepared, at="2026-09-15")

    # 거부했으면 아무것도 쓰지 않았어야 한다.
    assert spark.read.format("delta").load(path).count() == 1


def test_거부는_바뀐_아티스트만_본다(spark, warehouse):
    """값을 바꾸지 않은 줄은 MERGE가 건드리지 않으므로 무해하다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "가수")], at="2026-09-15")

    prepared = dim_artist.prepare(
        spark.createDataFrame([_artist(MBID_A, "가수")], DIM_INPUT_SCHEMA),
        at="2026-09-15", release="r2")

    assert dim_artist.unclosable(spark, path, prepared, at="2026-09-15") == 0
    dim_artist.merge(spark, path, prepared, at="2026-09-15")
    assert spark.read.format("delta").load(path).count() == 1


@pytest.mark.parametrize("broadcast", [True, False])
def test_브로드캐스트_여부가_결과를_바꾸지_않는다(spark, warehouse, broadcast):
    """S4가 이 플래그로 조인 전략을 비교한다. 전략이 결과를 바꾸면
    비교가 성립하지 않는다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "옛 이름")],
                   at="2026-09-15")

    prepared = dim_artist.prepare(
        spark.createDataFrame([_artist(MBID_A, "새 이름")], DIM_INPUT_SCHEMA),
        at="2026-09-16", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-16",
                     broadcast=broadcast)

    dim = spark.read.format("delta").load(path)
    assert dim.count() == 2
    assert dim.filter(dim[scd2.IS_CURRENT]).first()["name"] == "새 이름"


# ── 시점 조인 ───────────────────────────────────────────────

LISTEN_SCHEMA = "listen_id long, artist_mbid string, listened_at timestamp"


def _listens(spark, days, *, mbid=MBID_A):
    rows = [(i, mbid, datetime(2026, 9, day, 12, 0))
            for i, day in enumerate(days)]
    return spark.createDataFrame(rows, LISTEN_SCHEMA)


def test_과거_청취가_당시_이름으로_조회된다(spark, warehouse):
    """SCD Type 2를 만든 이유 그 자체다. 개명 전후가 **서로 달라야** 검증이 된다.
    한쪽만 보면 조인이 통째로 망가져 늘 옛 이름만 주어도 통과한다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "옛 이름")],
                   at="2026-09-01")
    prepared = dim_artist.prepare(
        spark.createDataFrame([_artist(MBID_A, "새 이름")], DIM_INPUT_SCHEMA),
        at="2026-09-10", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-10")

    dim = spark.read.format("delta").load(path)
    joined = fact_listens.with_artist_name(_listens(spark, [5, 15]), dim)
    names = {row["listen_id"]: row["artist_name"] for row in joined.collect()}

    assert names[0] == "옛 이름"
    assert names[1] == "새 이름"


def test_개명한_날의_청취가_두_줄에_걸리지_않는다(spark, warehouse):
    """구간을 [valid_from, valid_to)로 정한 이유다. 양끝을 다 포함하면
    이름이 바뀐 그 시각의 청취만 두 줄에 걸려 집계가 그때만 두 배가 된다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "옛 이름")],
                   at="2026-09-01")
    prepared = dim_artist.prepare(
        spark.createDataFrame([_artist(MBID_A, "새 이름")], DIM_INPUT_SCHEMA),
        at="2026-09-10", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-10")

    dim = spark.read.format("delta").load(path)
    joined = fact_listens.with_artist_name(_listens(spark, [10]), dim)

    assert joined.count() == 1
    assert joined.first()["artist_name"] == "새 이름"


def test_시점_판정이_scd2_covers와_같다(spark, warehouse):
    """같은 구간과 같은 시점을 두 구현에 넣어 대조한다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "옛 이름")],
                   at="2026-09-01")
    prepared = dim_artist.prepare(
        spark.createDataFrame([_artist(MBID_A, "새 이름")], DIM_INPUT_SCHEMA),
        at="2026-09-10", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-10")

    old = scd2.Version(key=MBID_A, values=("옛 이름",),
                       valid_from="2026-09-01", valid_to="2026-09-10",
                       is_current=False)
    new = scd2.Version(key=MBID_A, values=("새 이름",),
                       valid_from="2026-09-10", valid_to=scd2.FAR_FUTURE)

    days = [2, 9, 10, 11, 28]
    dim = spark.read.format("delta").load(path)
    joined = fact_listens.with_artist_name(_listens(spark, days), dim)
    names = {row["listen_id"]: row["artist_name"] for row in joined.collect()}

    for index, day in enumerate(days):
        at = f"2026-09-{day:02d}"
        expected = "옛 이름" if scd2.covers(old, at) else None
        if scd2.covers(new, at):
            expected = "새 이름"
        assert names[index] == expected, at


def test_차원에_없는_아티스트는_이름이_NULL로_남는다(spark, warehouse):
    """left 조인이므로 청취가 사라지지는 않는다. placeholder를 만드는 이유는
    NULL이 "모른다"와 "없다"를 구분하지 못하기 때문이다."""
    path = _create(spark, warehouse, [_artist(MBID_B, "다른 가수")],
                   at="2026-09-01")
    dim = spark.read.format("delta").load(path)

    joined = fact_listens.with_artist_name(_listens(spark, [5]), dim)
    assert joined.count() == 1
    assert joined.first()["artist_name"] is None


# ── placeholder ─────────────────────────────────────────────

def test_차원에_없는_아티스트를_찾는다(spark, warehouse):
    """NULL은 세지 않는다. 아티스트를 모르는 청취는 "차원에 없는 아티스트"가
    아니라 "아티스트가 없는 청취"다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "가수")], at="2026-09-01")
    dim = spark.read.format("delta").load(path)

    prepared = spark.createDataFrame(
        [(1, MBID_A), (2, MBID_B), (3, MBID_B), (4, None)],
        "listen_id long, artist_mbid string")

    missing = fact_listens.missing_artists(prepared, dim)
    assert [row["artist_mbid"] for row in missing.collect()] == [MBID_B]


def test_placeholder가_어떤_시점의_청취든_덮는다(spark, warehouse):
    """valid_from을 먼 과거로 두지 않으면 자리를 만들어 놓고도 조인이 안 된다."""
    path = _create(spark, warehouse, [_artist(MBID_A, "가수")], at="2026-09-01")
    dim = spark.read.format("delta").load(path)

    prepared = spark.createDataFrame(
        [(1, MBID_B)], "listen_id long, artist_mbid string")
    rows = fact_listens.placeholders(
        fact_listens.missing_artists(prepared, dim), dim)

    assert rows.columns == dim.columns
    row = rows.first()
    assert row["name"] == fact.PLACEHOLDER_NAME
    assert row[scd2.VALID_FROM] == fact.PLACEHOLDER_VALID_FROM
    assert row[scd2.VALID_TO] == scd2.FAR_FUTURE
    assert row[scd2.IS_CURRENT] is True


def test_placeholder를_넣으면_이름이_붙는다(spark, warehouse):
    """자리를 만든 뒤에 팩트를 써야 한다. 순서가 반대면 불변식을 확인하는
    순간에 고아 아티스트가 있는 상태가 된다.

    자리를 만들기 **전**에는 이름이 NULL이고, 만든 **뒤**에는 눈에 보이는
    문자열이 된다. NULL이면 "이름이 없는 아티스트"인지 "차원에 없는
    아티스트"인지 구분되지 않는다.
    """
    path = _create(spark, warehouse, [_artist(MBID_A, "가수")], at="2026-09-01")
    listens = _listens(spark, [5], mbid=MBID_B)

    before = fact_listens.with_artist_name(
        listens, spark.read.format("delta").load(path))
    assert before.first()["artist_name"] is None

    dim = spark.read.format("delta").load(path)
    rows = fact_listens.placeholders(
        fact_listens.missing_artists(listens, dim), dim)
    rows.write.format("delta").mode("append").save(path)

    after = fact_listens.with_artist_name(
        listens, spark.read.format("delta").load(path))
    assert after.count() == 1
    assert after.first()["artist_name"] == fact.PLACEHOLDER_NAME

    assert dim_artist.invariants(spark, path) == {
        "keys_without_exactly_one_current": 0,
        "keys_with_no_current": 0,
        "rows_with_invalid_range": 0,
    }


def test_placeholder를_닫으면_과거_청취에_거짓_이력이_남는다(spark, warehouse):
    """**아직 정하지 않은 것을 드러내는 테스트다.** 지금 동작을 고정한다.

    placeholder는 "아는 것이 MBID뿐"이라는 뜻이다. 다음 릴리스에 진짜 이름이
    들어오면 SCD2 병합이 그것을 **개명으로 취급해** placeholder 줄을 닫고
    새 줄을 연다.

    그 결과 1970-01-01 ~ 릴리스일 구간이 "(알 수 없는 아티스트)"라는
    **이름을 가진 이력**이 된다. 개명 전 청취를 조회하면 그 이름이 나온다.
    이 차원이 답하려는 질문은 "그때 뭐라고 불렸는가"인데, 그때 그렇게 불린
    적은 없다. 우리가 몰랐을 뿐이다.

    **모른다는 것과 그렇게 불렸다는 것은 다르다.** 이력이 아니라 무지이므로
    Type 1로 덮는 것이 맞을 수 있다. 정하려면 dim에 없는 661개가 어떤 성격인지
    (병합되어 사라진 것인지, 릴리스보다 새 것인지) 알아야 한다. 아직 재지 않았다
    (docs/measurements/2026-09-15-fact-grain.md의 "아직 재지 않은 것").

    지금 동작을 테스트로 고정해 둔다. 바꿀 때 무엇이 달라지는지 보이게 된다.
    """
    path = _create(spark, warehouse, [_artist(MBID_A, "가수")], at="2026-09-01")
    listens = _listens(spark, [5], mbid=MBID_B)

    dim = spark.read.format("delta").load(path)
    fact_listens.placeholders(
        fact_listens.missing_artists(listens, dim), dim
    ).write.format("delta").mode("append").save(path)

    prepared = dim_artist.prepare(
        spark.createDataFrame(
            [_artist(MBID_A, "가수"), _artist(MBID_B, "이제 아는 가수")],
            DIM_INPUT_SCHEMA),
        at="2026-09-20", release="r2")
    dim_artist.merge(spark, path, prepared, at="2026-09-20")

    # 릴리스 이후의 청취는 진짜 이름을 얻는다.
    later = fact_listens.with_artist_name(
        _listens(spark, [25], mbid=MBID_B),
        spark.read.format("delta").load(path))
    assert later.first()["artist_name"] == "이제 아는 가수"

    # 릴리스 전의 청취는 placeholder 이름에 묶인 채로 남는다.
    earlier = fact_listens.with_artist_name(
        listens, spark.read.format("delta").load(path))
    assert fact.is_placeholder(earlier.first()["artist_name"])
