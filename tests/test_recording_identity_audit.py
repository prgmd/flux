import importlib.util
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
spec = importlib.util.spec_from_file_location("identity_audit", Path(__file__).resolve().parents[1] /
                                             "scripts/investigations/audit_recording_identity.py")
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)


@pytest.fixture
def connection():
    with duckdb.connect() as c:
        c.execute("""create table metadata as select * from (values
            ('a','Song','Artist','r'),('a','Song','Artist','r'),
            ('b',' song ','ARTIST','r'),('c','Song','Artist','different-recording'),
            ('x','Other','Artist','x1'),('x','Other','Artist','x2'),
            ('empty','','Artist',NULL)) t(recording_msid,track_name,artist_name,recording_mbid)""")
        c.execute("""create table detail as select user_name,recording_msid,
            cast(stamp as timestamp) listened_at,strftime(cast(stamp as timestamp),'%Y-%m') listened_month
            from (values ('u','a','2026-01-01 00:00:00'),('u','b','2026-01-01 00:00:00'),
            ('u','b','2026-01-01 00:04:00'),('v','a','2026-01-01 00:00:00'),
            ('u','a','2026-02-01 00:00:00'),('u','c','2026-01-01 00:00:00'),
            ('u','x','2026-01-01 00:00:00')) t(user_name,recording_msid,stamp)""")
        c.execute("""create table monthly as select user_name,recording_msid,listened_month,
            count(*)::bigint play_count from detail group by all""")
        yield c


def test_mbid_mapping_avoids_metadata_fanout_and_excludes_conflicts(connection):
    audit.prepare_mapping(connection, "mbid")
    assert connection.execute("select recording_msid,song_key from mapping order by 1").fetchall() == [("a", "r"), ("b", "r")]


def test_same_recording_merge_preserves_repeats_but_same_event_reduces_count(connection):
    audit.prepare_mapping(connection, "mbid")
    result = audit.measure_policy(connection)
    assert result["monthly_rows_before"] == 6
    assert result["monthly_rows_after_merge"] == 5
    assert result["plays_before"] == 7
    assert result["plays_after_merge"] == 7
    assert result["duplicate_event_groups"] == 1
    assert result["extra_event_rows"] == 1
    assert result["plays_after_event_rule"] == 6
    assert result["affected_user_song_months"] == 1
    assert result["monthly_detail_mismatches"] == 0
    assert connection.execute("select plays_before,extra_rows from affected_months").fetchall() == [(3, 1)]


def test_name_match_is_only_a_candidate_and_can_merge_different_mbids(connection):
    audit.prepare_mapping(connection, "name")
    assert connection.execute("select count(*) from mapping").fetchone()[0] == 3
    result = audit.measure_policy(connection)
    assert result["extra_event_rows"] == 2
    assert result["plays_after_event_rule"] == 5
    assert result["plays_after_merge"] == 7


def test_monthly_source_mismatch_is_detected(connection):
    connection.execute("update monthly set play_count=play_count+1 where recording_msid='b'")
    audit.prepare_mapping(connection, "mbid")
    with pytest.raises(ValueError, match="monthly/detail"):
        audit.measure_policy(connection)


def test_existing_same_msid_duplicates_are_not_mislabeled_as_cross_msid(connection):
    connection.execute("insert into detail select * from detail where recording_msid='b' limit 1")
    audit.prepare_mapping(connection, "mbid")
    with pytest.raises(ValueError, match="duplicate user/MSID/timestamp"):
        audit.measure_policy(connection)


def test_numeric_working_keys_are_translated_back_in_anonymous_evidence(connection):
    for name in ("recording_mbid", "track_name", "artist_name", "client_name", "_source_dump"):
        connection.execute(f"alter table detail add column {name} varchar")
    audit.prepare_mapping(connection, "mbid")
    audit.measure_policy(connection)
    result = audit.evidence(connection)
    assert result["top_songs"][0]["song_key"] == "r"
    assert result["top_songs"][0]["extra_rows"] == 1
    assert {r["song_key"] for r in result["event_examples"]} == {"r"}
    assert {r["recording_msid"] for r in result["event_examples"]} == {"a", "b"}
    assert all("user_name" not in r for r in result["event_examples"])
