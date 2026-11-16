import hashlib
import json

import pytest

from flux.common import queryrun
from flux.common.active_release import ASSETS, active, publish, rollback


def put(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def candidate(root, name):
    release = root / "releases" / name
    tables = {path: {"version": 0} for kind, path in ASSETS.values() if kind == "delta_scan"}
    marts = {alias: {"matches": True, "rows": 1}
             for alias, (kind, _) in ASSETS.items() if kind == "read_parquet"}
    files = {}
    for kind, path in ASSETS.values():
        if kind == "delta_scan":
            path += "/_delta_log/00000000000000000000.json"
        files[path] = put(release / path, {})
    files["evidence/refresh.json"] = put(release / "evidence/refresh.json", {
        "completed": True, "gold_matches": True, "gold_rows": dict.fromkeys(marts, 1),
    })
    files["evidence/gold.json"] = put(release / "evidence/gold.json", {
        "completed": True, "comparisons": marts,
    })
    files["evidence/silver.json"] = put(release / "evidence/silver.json", {
        "duplicate_monthly_keys": 0, "missing_metadata": 0, "invalid_monthly_rows": 0,
        "independent_recompute": {"mismatches": 0, "duplicate_keys": 0},
    })
    put(release / "release.json", {"release": name, "files": files, "tables": tables,
                                  "gold_rows": dict.fromkeys(marts, 1)})
    return release


def test_publish_and_rollback_routes_both_layers(tmp_path):
    release = candidate(tmp_path, "2026-11-16")
    assert active(tmp_path) is None
    publish(tmp_path, release.name)
    assert active(tmp_path) == release
    sql = queryrun.expand(
        "select * from {bronze}, {monthly_user_recordings}, {yearly_song_metrics}, {silver}",
        tmp_path, release=active(tmp_path),
    )
    assert f"delta_scan('{release.as_posix()}/bronze/raw_listens')" in sql
    assert f"delta_scan('{release.as_posix()}/silver/monthly_user_recordings')" in sql
    assert f"read_parquet('{release.as_posix()}/gold/year/song_metrics.parquet')" in sql
    assert f"delta_scan('{tmp_path.as_posix()}/silver/listens_normalized')" in sql
    rollback(tmp_path, release.name)
    assert active(tmp_path) is None


def test_tampered_candidate_preserves_current(tmp_path):
    first = candidate(tmp_path, "2026-11-16")
    publish(tmp_path, first.name)
    second = candidate(tmp_path, "2026-11-17")
    (second / "gold/year/song_metrics.parquet").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="changed"):
        publish(tmp_path, second.name)
    assert active(tmp_path) == first


def test_rejects_incomplete_gold_and_corrupt_pointer(tmp_path):
    release = candidate(tmp_path, "2026-11-16")
    manifest = json.loads((release / "release.json").read_text(encoding="utf-8"))
    manifest["files"]["evidence/gold.json"] = put(release / "evidence/gold.json", {
        "completed": True, "comparisons": {},
    })
    put(release / "release.json", manifest)
    with pytest.raises(ValueError, match="Gold"):
        publish(tmp_path, release.name)
    assert active(tmp_path) is None
    put(tmp_path / "ops/current_release.json", {"active": {"release": "../outside"}, "previous": None})
    with pytest.raises(ValueError):
        active(tmp_path)


def test_rollback_selects_previous_release_and_checks_expected(tmp_path):
    first = candidate(tmp_path, "2026-11-16")
    second = candidate(tmp_path, "2026-11-17")
    publish(tmp_path, first.name)
    publish(tmp_path, second.name)
    with pytest.raises(ValueError, match="active"):
        rollback(tmp_path, first.name)
    rollback(tmp_path, second.name)
    assert active(tmp_path) == first


def test_monthly_alias_without_release_is_rejected(tmp_path):
    with pytest.raises(ValueError, match="release"):
        queryrun.expand("select * from {monthly_coverage}", tmp_path)


def test_aborted_writer_lock_prevents_switch(tmp_path):
    release = candidate(tmp_path, "2026-11-16")
    put(tmp_path / "ops/current_release.lock", {})
    with pytest.raises(FileExistsError):
        publish(tmp_path, release.name)
    assert active(tmp_path) is None


def test_republish_detects_a_changed_active_file(tmp_path):
    release = candidate(tmp_path, "2026-11-16")
    publish(tmp_path, release.name)
    (release / "gold/year/song_metrics.parquet").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="changed"):
        publish(tmp_path, release.name)
