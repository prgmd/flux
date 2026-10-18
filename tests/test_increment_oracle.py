import json

import pytest

duckdb = pytest.importorskip("duckdb")

from scripts.drills.verify_increment_counts import compare, local_files, verify_inputs


@pytest.fixture
def snapshots(tmp_path):
    connection = duckdb.connect()
    silver = tmp_path / "old-silver.parquet"
    actual = tmp_path / "counts.parquet"

    def write(path, query):
        target = path.as_posix().replace("'", "''")
        connection.execute(f"COPY ({query}) TO '{target}' (FORMAT PARQUET)")

    write(silver, "SELECT '1' AS user_id, 'song' AS recording_msid FROM range(2)")
    yield connection, silver, actual, write
    connection.close()


def test_pinned_files_ignore_newer_unlisted_data(snapshots, tmp_path):
    connection, silver, actual, write = snapshots
    write(actual, "SELECT '1' AS user_id, 'song' AS recording_msid, 2 AS play_count")
    snapshot = {"files": [str(silver)]}
    write(tmp_path / "newer-silver.parquet", "SELECT 'other' AS user_id, 'song' AS recording_msid")
    assert compare(connection, local_files(snapshot, tmp_path), [str(actual)])["ok"]


@pytest.mark.parametrize("query", [
    "SELECT '1' AS user_id, 'song' AS recording_msid, 3 AS play_count",
    "SELECT '2' AS user_id, 'song' AS recording_msid, 2 AS play_count",
    "SELECT '1' AS user_id, 'song' AS recording_msid, 2 AS play_count FROM range(2)",
    "SELECT '1' AS user_id, 'song' AS recording_msid, -1 AS play_count",
])
def test_incorrect_counts_or_keys_are_rejected(snapshots, query):
    connection, silver, actual, write = snapshots
    write(actual, query)
    assert not compare(connection, [str(silver)], [str(actual)])["ok"]


def test_snapshot_cannot_read_outside_data_root(tmp_path):
    with pytest.raises(ValueError):
        local_files({"files": [str(tmp_path.parent / "outside.parquet")]}, tmp_path)


@pytest.mark.parametrize("wrong_quarantine", [False, True])
def test_input_quality_is_checked_independently(tmp_path, wrong_quarantine):
    staging = tmp_path / "scratch" / "input" / "staging"
    staging.mkdir(parents=True)
    document = {"user_name": "u", "timestamp": 100, "track_metadata": {
        "additional_info": {"ms_played": 29999}}}
    (staging / "month.listens").write_text(json.dumps(document) + "\ninvalid-json\n", encoding="utf-8")
    report = {"input_plan": [{"slot": "first", "work": "input"}], "runs": {
        "first.first": {"ingested_at": "2026-10-18T00:00:00+00:00",
                        "bronze": {"source_lines": 2, "invalid": 1},
                        "quarantine": {} if wrong_quarantine else {"재생 시간이 30초 미만": 1}}}}
    if wrong_quarantine:
        with pytest.raises(AssertionError):
            verify_inputs(report, tmp_path)
    else:
        assert verify_inputs(report, tmp_path)["first"]["ok"]
