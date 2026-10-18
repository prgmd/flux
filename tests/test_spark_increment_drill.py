import json
from datetime import datetime, timezone

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from flux.common import config
from flux.bronze import ingest, schema as bronze_schema
from flux.silver import build, schema, user_recording_counts
from scripts.drills.new_dump_increment import main

pytestmark = pytest.mark.spark


def test_committed_merge_resumes_and_second_dump_does_not_double_count(spark, warehouse, monkeypatch):
    monkeypatch.setenv("FLUX_STORAGE_ROOT", warehouse("drill-root"))
    root = config.local_root()

    def listen(user, at):
        return {"user_id": user, "user_name": f"user-{user}", "timestamp": at,
                "recording_msid": "same-song", "track_metadata": {
                    "track_name": "song", "artist_name": "artist"}}

    def source(path, documents):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(json.dumps(doc) for doc in documents) + "\n", encoding="utf-8")

    original = root / "original.listens"
    source(original, [listen(1, 1700000000)])
    raw = ingest.build_dataframe(spark, str(original), source_dump="baseline",
                                source_file=original.name, ingested_at=datetime.now(timezone.utc))
    valid, _ = ingest.split(raw)
    bronze = config.bronze(bronze_schema.TABLE_NAME)
    ingest.write(valid, bronze, source_dump="baseline")
    silver, _ = build.build(spark, bronze, built_at=datetime.now(timezone.utc))
    build.write(silver, config.silver(schema.TABLE_NAME), partition=schema.PARTITION_COLUMN)
    source(root / "scratch" / "experiment" / "staging" / "month.listens",
           [listen(1, 1700000300), listen(1, 1700000300)])
    source(root / "scratch" / "next-input" / "staging" / "month.listens",
           [listen(1, 1700000600), listen(2, 1700000600)])
    arguments = ["--dump", "dump-a", "--work", "experiment",
                 "--next-dump", "dump-b", "--next-work", "next-input"]
    with pytest.raises(RuntimeError, match="주입한 중단"):
        main(arguments + ["--interrupt-after-merge"], spark=spark)
    report_path = root / "scratch" / "experiment" / "result.json"
    interrupted = json.loads(report_path.read_text(encoding="utf-8"))
    assert "merge" not in interrupted["runs"]["first.first"]
    assert main(arguments + ["--resume"], spark=spark) == 0
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["runs"]["first.first"]["merge"]["metrics"]["numTargetRowsInserted"] == 1
    assert report["runs"]["second.first"]["merge"]["metrics"]["numTargetRowsInserted"] == 2
    assert report["production_identity_before"] == report["production_identity_after"]
    final = report["runs"]["second.replay"]["snapshot"]
    assert final["silver"]["rows"] == 4
    actual = spark.read.format("delta").option("versionAsOf", final["counts"]["version"]).load(
        final["counts"]["path"])
    assert {(row.user_id, row.recording_msid): row.play_count for row in actual.collect()} == {
        ("1", "same-song"): 3, ("2", "same-song"): 1}
