import importlib.util
from pathlib import Path

import pytest


def module():
    path = Path(__file__).resolve().parents[1] / "scripts/migrations/rebuild_playback.py"
    spec = importlib.util.spec_from_file_location("rebuild_playback", path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def test_rebuild_rejects_overlapping_roots(tmp_path):
    rebuild = module()
    for other in (tmp_path, tmp_path / "child", tmp_path.parent):
        with pytest.raises(ValueError):
            rebuild.separate(tmp_path, other)
    rebuild.separate(tmp_path / "source", tmp_path / "target")


def test_snapshot_records_table_identity_and_detects_log_change(tmp_path):
    import json

    rebuild = module()
    table = tmp_path / "silver/listens_normalized/_delta_log"
    table.mkdir(parents=True)
    log = table / "00000000000000000000.json"
    log.write_text(json.dumps({"metaData": {"id": "new-id", "configuration": {"delta.enableChangeDataFeed": "true"}}}))
    first = rebuild.identity(tmp_path)
    assert first["silver/listens_normalized"]["id"] == "new-id"
    assert first["silver/listens_normalized"]["version"] == 0
    log.write_text(log.read_text() + '\n' + json.dumps({"commitInfo": {"operation": "WRITE"}}))
    assert rebuild.identity(tmp_path) != first
