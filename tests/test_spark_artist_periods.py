from datetime import date, datetime, timezone

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from flux.gold import artist_periods_build as periods

pytestmark = pytest.mark.spark
SCHEMA = "artist_mbid string, user_id string, listened_at timestamp, _source_dump string, _record_id long"


def frame(spark, rows):
    def timestamp(value):
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

    return spark.createDataFrame([
        (artist, user, timestamp(at), "dump", key)
        for artist, user, at, key in rows
    ], SCHEMA)


def test_periods_preserve_repeats_and_count_weekly_users_directly(spark):
    source = frame(spark, [
        ("a", "u", "2026-12-28T12:00:00", 1),
        ("a", "u", "2026-12-28T12:00:01", 2),
        ("a", "u", "2026-12-29T12:00:00", 3),
        ("b", "v", "2026-12-28T13:00:00", 4),
        ("b", "w", "2026-12-28T14:00:00", 5),
        ("c", "x", "2026-12-28T15:00:00", 6),
        ("d", "y", "2026-12-28T15:00:00", 7),
        (None, "missing", "2026-12-28T16:00:00", 8),
        ("collab", "u", "2026-12-28T12:00:00", 1),
        ("a", "u", "2027-01-03T23:59:59", 9),
        ("a", "u", "2027-01-04T00:00:00", 10),
    ])
    rows = periods.build(source).collect()
    weekly = {r.artist_mbid: r for r in rows if r.period == "week" and r.period_start == date(2026, 12, 28)}
    assert weekly["a"].listens == 4
    assert weekly["a"].listeners == 1
    assert weekly["a"].top_user_listens == 4
    assert weekly["a"].top_user_share == 1.0
    assert weekly["b"].rank == 1
    assert weekly["a"].rank == 2
    assert weekly["b"].top_user_share == 0.5
    assert weekly["c"].rank < weekly["d"].rank
    assert weekly["a"].period_end == date(2027, 1, 4)
    assert weekly["a"].period_listens == 9
    assert weekly["a"].linked_listens == 8
    assert weekly["a"].period_users == 6
    assert weekly["a"].linked_users == 5
    assert weekly["a"].coverage == pytest.approx(8 / 9)
    daily = [r for r in rows if r.period == "day" and r.artist_mbid == "a" and r.period_start < date(2027, 1, 4)]
    assert sum(r.listeners for r in daily) == 3
    assert sum(r.listens for r in daily) == weekly["a"].listens
    assert all(r.artist_mbid is not None for r in rows)
    assert any(r.period == "week" and r.period_start == date(2027, 1, 4) for r in rows)


def test_refresh_pins_source_and_replaces_late_changed_deleted_periods(spark, warehouse):
    from delta.tables import DeltaTable

    source_path, target = warehouse("fact"), warehouse("periods")
    initial = frame(spark, [("a", "u", "2026-10-12T23:59:59", 1)])
    initial.write.format("delta").save(source_path)
    assert periods.refresh(spark, source_path, target)["fact_version"] == 0
    def values():
        return (spark.read.format("delta").load(target).drop("_built_at")
                .orderBy("period", "period_start", "artist_mbid").collect())

    first = values()
    periods.refresh(spark, source_path, target)
    assert values() == first
    updated = frame(spark, [("b", "v", "2026-10-12T23:59:59", 2),
                            ("b", "w", "2026-10-13T00:00:00", 3)])
    updated.write.format("delta").mode("overwrite").save(source_path)
    periods.refresh(spark, source_path, target, fact_version=0)
    assert values() == first
    assert periods.refresh(spark, source_path, target)["fact_version"] == 1
    rows = spark.read.format("delta").load(target).collect()
    assert {r.artist_mbid for r in rows} == {"b"}
    assert next(r for r in rows if r.period == "week").listeners == 2
    initial.limit(0).write.format("delta").mode("overwrite").save(source_path)
    periods.refresh(spark, source_path, target)
    assert spark.read.format("delta").load(target).count() == 0
    metadata = DeltaTable.forPath(spark, target).history(1).first()["userMetadata"]
    assert '"fact_version": 2' in metadata


def test_requires_utc_and_rejects_writing_source(spark, warehouse):
    source = frame(spark, [("a", "u", "2026-10-12T00:00:00", 1)])
    spark.conf.set("spark.sql.session.timeZone", "Asia/Seoul")
    try:
        with pytest.raises(ValueError, match="UTC"):
            periods.build(source)
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    with pytest.raises(ValueError):
        periods.refresh(spark, warehouse("fact"), warehouse("fact"))


def test_offsets_are_assigned_to_utc_day_and_week(spark):
    source = frame(spark, [("a", "u", "2026-10-12T00:30:00+09:00", 1)])
    rows = {r.period: r for r in periods.build(source).collect()}
    assert rows["day"].period_start == date(2026, 10, 11)
    assert rows["week"].period_start == date(2026, 10, 5)


def test_weekly_maximum_is_not_sum_of_daily_maxima(spark):
    source = frame(spark, [
        ("a", "u", "2026-10-12T00:00:00", 1),
        ("a", "u", "2026-10-12T01:00:00", 2),
        ("a", "v", "2026-10-13T00:00:00", 3),
        ("a", "v", "2026-10-13T01:00:00", 4),
    ])
    rows = periods.build(source).collect()
    week = next(r for r in rows if r.period == "week")
    assert (week.listens, week.listeners, week.top_user_listens, week.top_user_share) == (4, 2, 2, 0.5)
    assert sum(r.top_user_listens for r in rows if r.period == "day") == 4
