"""날짜 차원과 사용자 차원의 Spark 표현이 flux.gold.dates의 규칙과 같은가."""

from datetime import date, datetime, timezone

import pytest

pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from flux.gold import dates, dim_date, dim_user

pytestmark = pytest.mark.spark

BUILT_AT = datetime(2026, 10, 5, tzinfo=timezone.utc)


def test_날짜_차원이_하루하루_규칙과_같다(spark):
    """ISO 주가 해를 넘는 두 경계(2025-12-29, 2027-01-01)를 기간에 넣었다."""
    first, last = date(2025, 12, 25), date(2027, 1, 5)
    rows = dim_date.build(spark, first, last).orderBy("date").collect()

    assert [r.asDict() for r in rows] == [dates.attributes(d) for d in dates.span(first, last)]


def test_날짜_차원은_청취가_없는_날도_줄이_있다(spark):
    listens = spark.createDataFrame(
        [(datetime(2026, 9, 1, 23, 30),), (datetime(2026, 9, 5, 0, 10),)], "listened_at timestamp")
    first, last = dim_date.listened_range(listens)

    assert (first, last) == (date(2026, 9, 1), date(2026, 9, 5))
    assert dim_date.build(spark, first, last).count() == 5


SILVER_SCHEMA = "user_id string, user_name string, listened_at timestamp"


def test_사용자마다_가장_최근_이름을_남긴다(spark):
    at = datetime(2026, 9, 1)
    rows = [
        ("u1", "old", datetime(2026, 1, 1)),
        ("u1", "new", datetime(2026, 9, 2)),
        ("u2", "alpha", at),
        ("u2", "beta", at),
    ]
    built = {r["user_id"]: r for r in
             dim_user.build(spark.createDataFrame(rows, SILVER_SCHEMA), built_at=BUILT_AT).collect()}

    for user in ("u1", "u2"):
        expected = dates.current_name((t, n) for u, n, t in rows if u == user)
        assert built[user]["user_name"] == expected
    assert built["u1"]["listens"] == 2
    assert built["u1"]["first_listened_at"] == datetime(2026, 1, 1)
    assert built["u1"]["last_listened_at"] == datetime(2026, 9, 2)


def test_팩트에만_있는_사용자를_센다(spark, warehouse):
    fact_path, user_path = warehouse("fact"), warehouse("dim_user")
    spark.createDataFrame([("u1",), ("u2",), ("u2",)], "user_id string") \
        .write.format("delta").save(fact_path)
    silver = spark.createDataFrame([("u1", "a", datetime(2026, 9, 1))], SILVER_SCHEMA)
    dim_user.write(dim_user.build(silver, built_at=BUILT_AT), user_path)

    assert dim_user.users_missing(spark, fact_path, user_path) == 1
