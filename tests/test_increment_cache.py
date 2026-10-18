import hashlib
import json

import pytest

from scripts.drills.cache_silver_readonly import copy_verified


def test_cache_matches_original_bytes_without_modifying_source(tmp_path):
    source = tmp_path / "source"
    (source / "_delta_log").mkdir(parents=True)
    body = b"immutable input"
    (source / "a.parquet").write_bytes(body)
    entries = [{"metaData": {"id": "original-id"}}, {"add": {"path": "a.parquet", "size": len(body)}}]
    log = source / "_delta_log" / "00000000000000000000.json"
    original = "\n".join(json.dumps(value) for value in entries).encode()
    log.write_bytes(original)
    certificate = copy_verified(source, tmp_path / "cache" / "silver")
    assert certificate["source_id"] == "original-id"
    assert certificate["log_sha256"] == hashlib.sha256(original).hexdigest()
    assert (tmp_path / "cache" / "silver" / "a.parquet").read_bytes() == body
    assert log.read_bytes() == original
    assert (source / "a.parquet").read_bytes() == body
    with pytest.raises(FileExistsError):
        copy_verified(source, tmp_path / "cache" / "silver")


@pytest.mark.parametrize("entry", [
    {"path": "a.parquet", "size": 999},
    {"path": "../outside.parquet", "size": 1},
])
def test_bad_size_and_escaping_paths_cannot_be_certified(tmp_path, entry):
    source = tmp_path / "source"
    (source / "_delta_log").mkdir(parents=True)
    (source / "a.parquet").write_bytes(b"data")
    actions = [{"metaData": {"id": "original-id"}}, {"add": entry}]
    (source / "_delta_log" / "00000000000000000000.json").write_text(
        "\n".join(json.dumps(value) for value in actions), encoding="utf-8")
    with pytest.raises((RuntimeError, ValueError)):
        copy_verified(source, tmp_path / "cache" / "silver")
    assert not (tmp_path / "cache" / "certificate.json").exists()
