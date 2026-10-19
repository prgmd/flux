import importlib.util
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")
pytestmark = pytest.mark.spark


@pytest.mark.parametrize("silver_only", [False, True])
def test_separate_rebuild_runs_all_stages_and_resumes(spark, tmp_path, monkeypatch, silver_only):
    script = Path(__file__).resolve().parents[1] / "scripts/migrations/rebuild_playback.py"
    spec = importlib.util.spec_from_file_location("playback_rebuild", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    production, work, export = [tmp_path / p for p in ("production", "work", "export")]
    artist = "b10bbbfc-cf9e-42e0-be17-e2c3e1d2600d"
    recording = "1557e001-08ec-405d-a0cf-6d2625e90af5"
    rows = []
    for key, user, at, played, dump in [(1, "u", 1000, 29999, "later"),
                                       (2, "u", 1000, 30000, "earlier"),
                                       (3, "u", 1001, None, "earlier"),
                                       (4, "v", 1002, "unknown", "earlier")]:
        document = {"user_id": user, "recording_msid": "track", "track_metadata": {
            "track_name": "song", "artist_name": "artist", "additional_info": {
                "recording_mbid": recording, "artist_mbids": [artist], "ms_played": played}}}
        rows.append((datetime(2026, 10, 19, tzinfo=timezone.utc), dump, "fixture", key,
                     at, user, json.dumps(document)))
    schema = "_ingested_at timestamp, _source_dump string, _source_file string, _record_id long, listened_at long, user_name string, raw_json string"
    spark.createDataFrame(rows, schema).write.format("delta").save((production / "bronze/raw_listens").as_uri())
    dim_schema = "artist_mbid string, name string, valid_from string, valid_to string, is_current boolean"
    spark.createDataFrame([(artist, "artist", "1970-01-01", "9999-12-31", True)], dim_schema).write.format("delta").save((production / "gold/dim_artist").as_uri())
    shutil.copytree(production, work)
    state = module.run(spark, production, work, export, silver_only=silver_only)
    assert state["completed"]
    assert state["production_before"] == module.identity(production)
    assert len(state["stages"]) == (2 if silver_only else 7)
    silver = spark.read.format("delta").load((export / "silver/listens_normalized").as_uri())
    assert {r._record_id for r in silver.select("_record_id").collect()} == {2, 3, 4}
    counts = spark.read.format("delta").load((export / "silver/user_recording_counts").as_uri())
    assert {r.user_id: r.play_count for r in counts.collect()} == {"u": 2, "v": 1}
    if silver_only:
        assert not (export / "gold").exists()
        assert module.run(spark, production, work, export, silver_only=True)["stages"] == state["stages"]
        import sys
        spec = importlib.util.spec_from_file_location("verify_silver", script.with_name("verify_playback_rebuild.py"))
        oracle = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(oracle)
        monkeypatch.setattr(sys, "argv", ["verify", "--production-root", str(production), "--rebuild-root", str(export), "--silver-only"])
        shutil.copytree(production / "bronze", export / "bronze")
        monkeypatch.setattr(sys, "argv", sys.argv + ["--bronze-root", str(export)])
        oracle.main()
        with pytest.raises(RuntimeError, match="scope changed"):
            module.run(spark, production, work, export)
        assert json.loads((export / "verification.json").read_text())["ok"]
        oracle.main()
        bronze_log = export / "bronze/raw_listens/_delta_log/00000000000000000000.json"
        bronze_log.write_text(bronze_log.read_text() + '\n' + json.dumps({"commitInfo": {"operation": "WRITE"}}))
        with pytest.raises(RuntimeError, match="Cached oracle Bronze"):
            oracle.main()
        return
    period = spark.read.format("delta").load((export / "gold/mart_period_artist").as_uri())
    assert all((r.listens, r.listeners, r.top_user_listens) == (3, 2, 2) for r in period.collect())
    assert module.run(spark, production, work, export, silver_only=silver_only)["stages"] == state["stages"]
    import sys
    spec = importlib.util.spec_from_file_location("verify_playback", script.with_name("verify_playback_rebuild.py"))
    oracle = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(oracle)
    monkeypatch.setattr(sys, "argv", ["verify", "--production-root", str(production), "--rebuild-root", str(export)])
    oracle.main()
    assert json.loads((export / "verification.json").read_text())["ok"]
