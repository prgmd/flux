"""청취 한 줄 따라가기의 조회. 작은 메모리 표로 같은 코드를 돌린다."""

import json
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb", reason="DuckDB가 없는 환경이다")

from flux.explorer import trace

TS = 1700000000
SAMPLE = Path("C:/Users/SSAFY/Desktop/jjh/flux-data/explorer/sample.duckdb")


def doc(msid="m1", artist="가수", track="곡", client="navidrome", mapping=None, lastfm=None):
    info = {"submission_client": client}
    if lastfm:
        info["lastfm_track_mbid"] = lastfm
    metadata = {"artist_name": artist, "track_name": track, "additional_info": info}
    if mapping:
        metadata["mbid_mapping"] = {"recording_mbid": mapping}
    return json.dumps({"user_name": "alice", "recording_msid": msid, "track_metadata": metadata})


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute("SET TimeZone='UTC'")
    connection.execute("create table bronze(_source_dump varchar, _record_id bigint, user_name varchar,"
                       " listened_at bigint, raw_json varchar)")
    connection.execute("create table silver(listened_at timestamptz, listened_month varchar, user_id varchar,"
                       " user_name varchar, recording_msid varchar, recording_mbid varchar, artist_mbids varchar[],"
                       " release_mbid varchar, mbid_source varchar, client_name varchar, track_name varchar,"
                       " artist_name varchar, release_name varchar, duration_ms bigint, _source_dump varchar,"
                       " _record_id bigint, _ingested_at timestamptz, _built_at timestamptz)")
    connection.execute("create table fact(_source_dump varchar, _record_id bigint, artist_mbid varchar)")
    connection.execute("create table dim_artist(artist_mbid varchar, name varchar, valid_from varchar,"
                       " valid_to varchar, is_current boolean)")

    rows = [
        ("d1", 5, "alice", TS, doc("m1", client="old-client")),
        ("d2", 5, "alice", TS, doc("m1", client="navidrome", mapping="11111111-1111-1111-1111-111111111111")),
        ("d1", 2, "alice", TS, doc("m2", artist="가수", track="곡")),
        ("d1", 3, "alice", TS + 100, doc("m3", artist="다른 가수", track="다른 곡", client="x")),
        ("d1", 9, "bob", TS, doc("m1")),
    ]
    connection.executemany("insert into bronze values (?, ?, ?, ?, ?)", rows)
    silver_rows = [
        (TS, "m1", "가수", "곡", "navidrome", "MAPPING", "d2", 5),
        (TS, "m2", "가수", "곡", "navidrome", "NONE", "d1", 2),
        (TS + 100, "m3", "다른 가수", "다른 곡", "x", "NONE", "d1", 3),
    ]
    for ts, msid, artist, track, client, source, dump, record in silver_rows:
        connection.execute(
            "insert into silver values (to_timestamp(?), '2023-11', '1', 'alice', ?, NULL, NULL, NULL, ?, ?, ?, ?,"
            " NULL, NULL, ?, ?, to_timestamp(0), to_timestamp(0))",
            [ts, msid, source, client, track, artist, dump, record])
    connection.execute("insert into fact values ('d2', 5, 'A'), ('d2', 5, 'B'), ('d1', 2, NULL), ('d1', 3, 'A')")
    connection.execute("insert into dim_artist values ('A', '옛 이름', '1970-01-01', '2023-11-14', false),"
                       " ('A', '새 이름', '2023-11-14', '9999-12-31', true), ('B', '밴드', '1970-01-01', '9999-12-31', true)")
    yield connection
    connection.close()


def test_이력에서_그_날짜의_줄을_찾는다():
    versions = [{"valid_from": "1970-01-01", "valid_to": "2023-11-14"}, {"valid_from": "2023-11-14", "valid_to": "9999-12-31"}]
    assert trace.version_at(versions, "2020-05-05") == 0
    assert trace.version_at(versions, "2024-01-01") == 1


def test_시작은_포함하고_끝은_제외한다():
    versions = [{"valid_from": "1970-01-01", "valid_to": "2023-11-14"}, {"valid_from": "2023-11-14", "valid_to": "9999-12-31"}]
    assert trace.version_at(versions, "2023-11-14") == 1
    assert trace.version_at(versions, "2023-11-13") == 0
    assert trace.version_at(versions, "1970-01-01") == 0


def test_이력이_없거나_덮지_못하면_None이다():
    assert trace.version_at([], "2024-01-01") is None
    assert trace.version_at([{"valid_from": "2025-01-01", "valid_to": "9999-12-31"}], "2024-01-01") is None


def test_목록은_한_사용자의_Silver_청취만_최근순으로_준다(con):
    rows = trace.listens(con, "alice")
    assert [r["recording_msid"] for r in rows] == ["m3", "m1", "m2"] or [r["recording_msid"] for r in rows][0] == "m3"
    assert {r["recording_msid"] for r in rows} == {"m1", "m2", "m3"}
    assert trace.listens(con, "bob") == []


def test_목록의_원본_줄_수는_같은_시각과_msid의_Bronze_줄_수다(con):
    copies = {r["recording_msid"]: r["copies"] for r in trace.listens(con, "alice")}
    assert copies == {"m1": 2, "m2": 1, "m3": 1}


def test_목록은_글자와_클라이언트로_거른다(con):
    assert {r["recording_msid"] for r in trace.listens(con, "alice", text="다른")} == {"m3"}
    assert {r["recording_msid"] for r in trace.listens(con, "alice", client="x")} == {"m3"}
    assert trace.listens(con, "alice", text="다른", client="navidrome") == []


def test_글자_거르기는_대소문자를_구분하지_않는다(con):
    con.execute("update silver set artist_name = 'The Band' where recording_msid = 'm2'")
    assert {r["recording_msid"] for r in trace.listens(con, "alice", text="the band")} == {"m2"}


def test_목록의_최대_줄_수를_지킨다(con):
    assert len(trace.listens(con, "alice", limit=2)) == 2


def test_줄_번호가_같아도_덤프까지_같아야_남은_줄이다(con):
    result = trace.trace(con, "alice", TS, "m1")
    assert [c["kept"] for c in result["copies"]].count(True) == 1


def test_같은_키의_원본_줄을_모두_찾고_남은_줄을_표시한다(con):
    result = trace.trace(con, "alice", TS, "m1")
    assert [(c["_source_dump"], c["_record_id"], c["kept"]) for c in result["copies"]] == [("d1", 5, False), ("d2", 5, True)]


def test_다른_사용자의_같은_시각_줄은_섞이지_않는다(con):
    result = trace.trace(con, "alice", TS, "m1")
    assert all(c["document"]["user_name"] == "alice" for c in result["copies"])
    assert len(result["copies"]) == 2


def test_같은_시각의_다른_msid를_따로_모은다(con):
    result = trace.trace(con, "alice", TS, "m1")
    assert [(r["msid"], r["artist_name"], r["track_name"]) for r in result["same_time_other_msid"]] == [("m2", "가수", "곡")]


def test_칸의_출처는_남은_줄의_JSON에서_읽는다(con):
    columns = {c["column"]: c for c in trace.trace(con, "alice", TS, "m1")["columns"]}
    candidates = dict(columns["recording_mbid"]["candidates"])
    assert candidates["$.track_metadata.mbid_mapping.recording_mbid"] == "11111111-1111-1111-1111-111111111111"
    assert candidates["$.track_metadata.additional_info.recording_mbid"] is None
    assert dict(columns["client_name"]["candidates"])["$.track_metadata.additional_info.submission_client"] == "navidrome"
    assert columns["client_name"]["silver_value"] == "navidrome"


def test_팩트는_아티스트마다_한_줄이고_NULL도_남는다(con):
    facts = trace.trace(con, "alice", TS, "m1")["facts"]
    assert [f["artist_mbid"] for f in facts] == ["A", "B"]
    assert [f["artist_mbid"] for f in trace.trace(con, "alice", TS, "m2")["facts"]] == [None]


def test_청취_날짜에_유효했던_이름을_가리킨다(con):
    fact = trace.trace(con, "alice", TS, "m1")["facts"][0]
    assert fact["versions"][fact["active"]]["name"] == "새 이름"
    con.execute("update silver set listened_at = to_timestamp(1600000000) where recording_msid = 'm1'")
    con.execute("update bronze set listened_at = 1600000000 where user_name = 'alice' and listened_at = ?", [TS])
    fact = trace.trace(con, "alice", 1600000000, "m1")["facts"][0]
    assert fact["versions"][fact["active"]]["name"] == "옛 이름"


def test_아티스트가_없는_팩트는_이력이_비어_있다(con):
    fact = trace.trace(con, "alice", TS, "m2")["facts"][0]
    assert fact["versions"] == []
    assert fact["active"] is None


def test_Silver에_없는_청취는_거부한다(con):
    with pytest.raises(LookupError):
        trace.trace(con, "alice", TS, "nope")


@pytest.mark.skipif(not SAMPLE.exists(), reason="표본 파일이 없다")
def test_실제_표본의_청취를_전부_따라갈_수_있다():
    real = duckdb.connect(str(SAMPLE), read_only=True)
    real.execute("SET TimeZone='UTC'")
    users = [r[0] for r in real.execute("select user_name from sample_users order by user_name limit 3").fetchall()]
    checked = 0
    for user in users:
        for row in trace.listens(real, user, limit=200):
            result = trace.trace(real, user, row["ts"], row["recording_msid"])
            assert sum(1 for c in result["copies"] if c["kept"]) == 1
            assert result["facts"]
            checked += 1
    assert checked > 0
