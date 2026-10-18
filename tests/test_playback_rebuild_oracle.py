import importlib.util
import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest


def module():
    path = Path(__file__).resolve().parents[1] / "scripts/migrations/verify_playback_rebuild.py"
    spec = importlib.util.spec_from_file_location("playback_oracle", path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def test_oracle_filters_before_picking_and_keeps_unknown_playback():
    con = duckdb.connect()
    con.execute("create table bronze(_source_dump varchar, _record_id bigint, user_name varchar, listened_at bigint, _ingested_at timestamp, raw_json varchar)")
    artist = "B10BBBFC-CF9E-42E0-BE17-E2C3E1D2600D"
    rows = []
    for dump, key, user, played in [("later", 1, "u", 29999), ("earlier", 2, "u", 30000),
                                    ("none", 3, "v", None), ("text", 4, "w", "bad"),
                                    ("negative", 5, "x", -1), ("boolean", 6, "y", True)]:
        doc = {"user_id": user, "recording_msid": "song", "track_metadata": {"additional_info": {
            "ms_played": played, "artist_mbids": [artist], "recording_mbid": artist}}}
        rows.append((dump, key, user, 1, datetime(2026, 10, 19), json.dumps(doc)))
    con.executemany("insert into bronze values (?, ?, ?, ?, ?, ?)", rows)
    module().prepare(con)
    assert {r[0] for r in con.execute("select _record_id from expected_silver").fetchall()} == {2, 3, 4, 6}
    assert con.execute("select artist_mbids from expected_silver where _record_id=2").fetchone()[0] == [artist.lower()]


def test_oracle_rejects_wrong_values_missing_keys_and_duplicate_keys():
    con = duckdb.connect()
    compare = module().compare
    assert compare(con, "select 1 as id, 2 as n", "select 1 as id, 2 as n", ["id"], ["n"])["mismatches"] == 0
    for actual in ("select 1 as id, 3 as n", "select 2 as id, 2 as n",
                   "select 1 as id, 2 as n union all select 1, 2"):
        with pytest.raises(AssertionError):
            compare(con, "select 1 as id, 2 as n", actual, ["id"], ["n"])


def test_empty_delta_uses_schema_without_parquet_files(tmp_path):
    log = tmp_path / "_delta_log"
    log.mkdir()
    schema = {"fields": [{"name": "n", "type": "long"},
                         {"name": "artists", "type": {"type": "array", "elementType": "string"}}]}
    (log / "00000000000000000000.json").write_text(json.dumps({"metaData": {"schemaString": json.dumps(schema)}}))
    con = duckdb.connect()
    module().read_table(con, "empty", tmp_path, [])
    assert con.execute('select * from "empty"').fetchall() == []
    assert con.execute('describe "empty"').fetchall()[0][1] == "BIGINT"
