"""급상승 마트의 Spark 표현이 mart.rising과 같은 순위를 내는가."""

import random
from datetime import date, datetime, timedelta, timezone

import pytest

pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from flux.gold import dates, dim_date, mart, mart_build

pytestmark = pytest.mark.spark

BUILT_AT = datetime(2026, 10, 5, tzinfo=timezone.utc)
DAILY_SCHEMA = "artist_mbid string, listened_date date, listens long, artist_name string"
WEEK = date(2026, 8, 17)


def _daily(rng):
    """세 주에 걸친 아티스트 40명의 일별 청취. 주마다 하루씩 흩어 넣는다."""
    rows = []
    for n in range(40):
        artist = f"artist-{n:02}"
        for week in range(3):
            start = WEEK - timedelta(days=7) + timedelta(days=7 * week)
            for day in rng.sample(range(7), 3):
                rows.append((artist, start + timedelta(days=day), rng.randint(0, 40), artist))
    return rows


def _weekly(rows, week_start):
    totals = {}
    for artist, day, listens, _ in rows:
        if week_start <= day < week_start + timedelta(days=7):
            totals[artist] = totals.get(artist, 0) + listens
    return totals


def test_주마다_순위가_규칙과_같다(spark):
    rows = _daily(random.Random(7))
    calendar = dim_date.build(spark, WEEK - timedelta(days=7), WEEK + timedelta(days=13))
    built = mart_build.rising(spark.createDataFrame(rows, DAILY_SCHEMA), calendar,
                              built_at=BUILT_AT).collect()

    for week_start in (WEEK, WEEK + timedelta(days=7)):
        expected = mart.rising(_weekly(rows, week_start),
                               _weekly(rows, week_start - timedelta(days=7)))
        actual = sorted((r for r in built if r["week_start"] == week_start),
                        key=lambda r: r["rank"])
        assert [(r["artist_mbid"], r["listens"], r["previous_listens"]) for r in actual] \
            == expected


def test_주의_시작은_날짜_차원의_월요일이다(spark):
    calendar = dim_date.build(spark, date(2026, 8, 10), date(2026, 8, 23))
    rows = [("a", date(2026, 8, 16), 20, "a"), ("a", date(2026, 8, 17), 50, "a")]
    built = mart_build.rising(spark.createDataFrame(rows, DAILY_SCHEMA), calendar,
                              built_at=BUILT_AT).collect()

    assert [(r["week_start"], r["listens"], r["previous_listens"]) for r in built] \
        == [(dates.attributes(date(2026, 8, 17))["week_start"], 50, 20)]
