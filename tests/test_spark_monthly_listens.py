import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from flux.silver import monthly_listens as monthly

pytestmark = pytest.mark.spark
NOW = datetime(2026, 10, 19, tzinfo=timezone.utc)
SCHEMA = "_ingested_at timestamp, _source_dump string, _source_file string, _record_id long, listened_at long, user_name string, raw_json string, ingested_date date"


def raw(number, timestamp, *, user="u", msid="song", played=None, dump="a", title="Song"):
    info = {} if played is None else {"ms_played": played}
    payload = {"user_id": "1", "recording_msid": msid,
               "track_metadata": {"track_name": title, "artist_name": "Artist",
                                  "additional_info": info}}
    return (NOW, dump, "file", number, timestamp, user, json.dumps(payload), NOW.date())


def values(spark, root):
    return {(r.user_name, r.recording_msid, r.listened_month): r.play_count
            for r in spark.read.format("delta").load(str(root / monthly.TABLE)).collect()}


def test_품질_중복제거_월별집계와_다음덤프_삭제_재실행을_검증한다(spark, tmp_path):
    source, root = str(tmp_path / "bronze"), tmp_path / "monthly"

    def write(rows):
        (spark.createDataFrame(rows, SCHEMA).write.format("delta")
         .option("delta.enableChangeDataFeed", "true").mode("overwrite").save(source))

    jan, feb = 1769903999, 1769904000
    a = raw(1, jan)
    b = raw(2, jan + 2, played=30000)
    short = raw(3, jan + 3, played=29999)
    write([a, b, short, raw(4, jan, dump="b")])
    monthly.refresh(spark, source, root, built_at=NOW)
    assert values(spark, root) == {("u", "song", "2026-01"): 1,
                                  ("u", "song", "2026-02"): 1}
    assert spark.read.format("delta").load(str(root / monthly.QUARANTINE)).count() == 1
    monthly.refresh(spark, source, root, built_at=NOW)
    assert sum(values(spark, root).values()) == 2

    write([a, b, short, raw(4, jan, dump="b"), raw(5, feb + 20, dump="c"),
           raw(6, jan, dump="c", title="Alternate")])
    monthly.refresh(spark, source, root, built_at=NOW)
    assert values(spark, root)[("u", "song", "2026-02")] == 2
    assert sum(values(spark, root).values()) == 3
    monthly.refresh(spark, source, root, built_at=NOW)
    assert sum(values(spark, root).values()) == 3

    write([a, raw(7, feb + 20, user="other", msid="other-song")])
    monthly.refresh(spark, source, root, built_at=NOW)
    assert values(spark, root) == {("u", "song", "2026-01"): 1,
                                  ("other", "other-song", "2026-02"): 1}
    assert spark.read.format("delta").load(str(root / monthly.QUARANTINE)).count() == 0
    assert {p.name for p in root.iterdir()} == {monthly.TABLE, monthly.METADATA,
                                              monthly.QUARANTINE}
    from flux.silver import build
    expected, _ = build.build(spark, source, built_at=NOW)
    expected = expected.groupBy(*monthly.KEY).count().withColumnRenamed("count", "play_count")
    actual = spark.read.format("delta").load(str(root / monthly.TABLE))
    assert actual.exceptAll(expected.select(*actual.columns)).count() == 0
    assert expected.select(*actual.columns).exceptAll(actual).count() == 0


def test_집계커밋_직후_중단되어도_재시도로_보조표를_완성한다(spark, tmp_path, monkeypatch):
    source, root = str(tmp_path / "bronze"), tmp_path / "monthly"
    (spark.createDataFrame([raw(1, 1769904000)], SCHEMA).write.format("delta")
     .option("delta.enableChangeDataFeed", "true").save(source))
    original = monthly.advance_metadata
    monkeypatch.setattr(monthly, "advance_metadata", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("중단")))
    with pytest.raises(RuntimeError, match="중단"):
        monthly.refresh(spark, source, root, built_at=NOW)
    monkeypatch.setattr(monthly, "advance_metadata", original)
    monthly.refresh(spark, source, root, built_at=NOW)
    assert sum(values(spark, root).values()) == 1
    assert monthly.ready(spark, root)
    (spark.createDataFrame([raw(2, 1769904100)], SCHEMA).write.format("delta")
     .mode("append").save(source))
    monkeypatch.setattr(monthly, "advance_metadata", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("중단")))
    with pytest.raises(RuntimeError, match="중단"):
        monthly.refresh(spark, source, root, built_at=NOW)
    assert not monthly.ready(spark, root)
    with pytest.raises(RuntimeError, match="완성되지"):
        monthly.pin_snapshot(spark, root)
    assert sum(values(spark, root).values()) == 2
    monkeypatch.setattr(monthly, "advance_metadata", original)
    monthly.refresh(spark, source, root, built_at=NOW)
    assert sum(values(spark, root).values()) == 2
    assert monthly.ready(spark, root)
    assert set(monthly.pin_snapshot(spark, root)) == {monthly.TABLE, monthly.METADATA, monthly.QUARANTINE}


def test_Bronze_변경이력이_없으면_추측해서_가산하지_않는다(spark, tmp_path):
    source, root = str(tmp_path / "bronze"), tmp_path / "monthly"
    spark.createDataFrame([raw(1, 1769904000)], SCHEMA).write.format("delta").save(source)
    monthly.refresh(spark, source, root, built_at=NOW)
    spark.createDataFrame([raw(2, 1769904100)], SCHEMA).write.format("delta").mode("append").save(source)
    with pytest.raises(monthly.FullRebuildNeeded, match="변경 이력"):
        monthly.refresh(spark, source, root, built_at=NOW)
    assert sum(values(spark, root).values()) == 1
    assert not (root / ".writer.lock").exists()


def test_이전_Silver의_재생시간_정책이_다르면_전환을_막는다(tmp_path, monkeypatch):
    import importlib.util
    from pathlib import Path

    directory = Path(__file__).resolve().parents[1] / "scripts/migrations"
    monkeypatch.syspath_prepend(str(directory))
    spec = importlib.util.spec_from_file_location("compact_monthly", directory / "compact_monthly_silver.py")
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    source, target = tmp_path / "verified", tmp_path / "output"
    source.mkdir()
    for name, value in {"verification.json": {"ok": True},
                        "completion.json": {"completed_at": "done"},
                        "verification-snapshots.json": {},
                        "rebuild.json": {"minimum_ms_played": 10000}}.items():
        (source / name).write_text(json.dumps(value), encoding="utf-8")
    with pytest.raises(ValueError, match="품질 기준"):
        migration.run(None, source, target)
    assert not target.exists()
