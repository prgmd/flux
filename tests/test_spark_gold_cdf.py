import json

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from flux.silver import monthly_listens as monthly
from flux.validation.silver_cdf_export import export

pytestmark = pytest.mark.spark


def create(spark, root):
    state = monthly.state_for("bounded-bronze", 0)
    frame = spark.createDataFrame([("u", "s1", "2026-01", 2)], "user_name string, recording_msid string, listened_month string, play_count long")
    monthly.create(frame, root / monthly.TABLE, state, partition="listened_month")
    monthly.create(spark.createDataFrame([("s1", "Song", "Artist", None, None, "none")],
                   "recording_msid string, track_name string, artist_name string, recording_mbid string, artist_mbids array<string>, mbid_source string"),
                   root / monthly.METADATA, state)
    monthly.create(spark.createDataFrame([], "reason string"), root / monthly.QUARANTINE, state)


def test_actual_delta_cdf_is_pinned_and_replays_without_changes(spark, tmp_path):
    root = tmp_path / "silver"
    create(spark, root)
    first = export(spark, root, tmp_path / "initial")
    assert first["completed"] is True
    cursor = first["source_cursor"]
    DeltaTable.forPath(spark, str(root / monthly.TABLE)).update(set={"play_count": F.lit(3)})
    monthly.touch(spark, root / monthly.TABLE, monthly.state_for("bounded-bronze", 0))
    second = export(spark, root, tmp_path / "changed", previous=cursor)
    assert second["completed"] is True, second.get("failure")
    payload = json.loads((tmp_path / "changed/payload.json").read_text())
    assert {row["_change_type"] for row in payload["cdf"]} == {"update_preimage", "update_postimage"}
    assert payload["before"][0]["play_count"] == 2
    assert payload["after"][0]["play_count"] == 3
    replay = export(spark, root, tmp_path / "replay", previous=second["source_cursor"])
    assert replay["completed"] and replay["cdf_rows"] == 0
    broken = json.loads(json.dumps(cursor)); broken["tables"][monthly.TABLE]["id"] = "replaced"
    with pytest.raises(ValueError): export(spark, root, tmp_path / "replaced", previous=broken)
    assert not (tmp_path / "replaced").exists()


def test_real_disabled_feed_and_incomplete_silver_are_rejected(spark, tmp_path):
    root = tmp_path / "silver"
    create(spark, root)
    first = export(spark, root, tmp_path / "initial")
    spark.sql(f"ALTER TABLE delta.`{root / monthly.TABLE}` SET TBLPROPERTIES ('delta.enableChangeDataFeed'='false')")
    DeltaTable.forPath(spark, str(root / monthly.TABLE)).update(set={"play_count": F.lit(4)})
    monthly.touch(spark, root / monthly.TABLE, monthly.state_for("bounded-bronze", 0))
    failed = export(spark, root, tmp_path / "missing", previous=first["source_cursor"])
    assert failed["completed"] is False
    assert "failure" in failed
    monthly.touch(spark, root / monthly.METADATA, monthly.state_for("bounded-bronze", 1))
    with pytest.raises(RuntimeError): export(spark, root, tmp_path / "partial")
    assert not (tmp_path / "partial").exists()
