"""표본 추출기. 원본을 작은 메모리 표로 대신해 같은 코드를 돌린다."""

import pytest

duckdb = pytest.importorskip("duckdb", reason="DuckDB가 없는 환경이다")

from flux.explorer import pick, sample

SOURCES = {name: f"src_{name}" for name in sample.SOURCE_NAMES}


@pytest.fixture
def con():
    connection = duckdb.connect()
    connection.execute("create table src_bronze(_source_dump varchar, _record_id bigint, user_name varchar, raw_json varchar)")
    connection.execute(
        "create table src_silver(_source_dump varchar, _record_id bigint, user_name varchar, user_id varchar,"
        " client_name varchar, artist_mbids varchar[])")
    connection.execute("create table src_fact(_source_dump varchar, _record_id bigint, user_id varchar, artist_mbid varchar)")
    connection.execute("create table src_dim_artist(artist_mbid varchar, name varchar, valid_from varchar)")
    connection.execute("create table src_quarantine(_source_dump varchar, _record_id bigint, reason varchar)")

    for user in ("alice", "bob", "carol"):
        for i in range(3):
            connection.execute("insert into src_bronze values ('d1', ?, ?, '{}')", [hash((user, i)) % 1000, user])
        connection.execute("insert into src_bronze values ('d2', 999, ?, '{}')", [user])
    connection.execute("insert into src_silver values ('d1', 1, 'alice', '1', 'ListenBrainz Archive Importer', ['A', 'B'])")
    connection.execute("insert into src_silver values ('d1', 2, 'alice', '1', 'navidrome', ['A'])")
    connection.execute("insert into src_silver values ('d1', 3, 'bob', '2', 'navidrome', [])")
    connection.execute("insert into src_silver values ('d1', 4, 'carol', '3', 'navidrome', NULL)")
    connection.execute("insert into src_fact values ('d1', 1, '1', 'A'), ('d1', 1, '1', 'B'), ('d1', 2, '1', 'A'),"
                       " ('d1', 3, '2', NULL), ('d1', 4, '3', 'Z')")
    connection.execute("insert into src_dim_artist values ('A', 'a1', '1970-01-01'), ('A', 'a2', '2026-01-01'),"
                       " ('B', 'b', '1970-01-01'), ('Z', 'z', '1970-01-01')")
    connection.execute("insert into src_bronze values ('dq', 77, 'alice', '{}')")
    connection.execute("insert into src_quarantine values ('dq', 77, '사유'), ('dq', 88, '다른 사용자')")
    yield connection
    connection.close()


def test_뽑은_사용자의_줄만_담는다(con, tmp_path):
    counts = sample.extract(con, SOURCES, {"alice": "가져오기 중심"}, tmp_path / "s.duckdb")
    assert counts["silver"] == 2
    assert counts["bronze"] == 5
    other = duckdb.connect(str(tmp_path / "s.duckdb"), read_only=True)
    assert other.execute("select distinct user_name from bronze").fetchall() == [("alice",)]
    assert other.execute("select distinct user_name from silver").fetchall() == [("alice",)]


def test_팩트는_사용자_번호로_찾는다(con, tmp_path):
    sample.extract(con, SOURCES, {"alice": "x"}, tmp_path / "s.duckdb")
    other = duckdb.connect(str(tmp_path / "s.duckdb"), read_only=True)
    assert other.execute("select count(*) from fact").fetchone()[0] == 3
    assert other.execute("select distinct user_id from fact").fetchall() == [("1",)]


def test_차원은_팩트에_나온_아티스트의_이력_전부를_담는다(con, tmp_path):
    sample.extract(con, SOURCES, {"alice": "x"}, tmp_path / "s.duckdb")
    other = duckdb.connect(str(tmp_path / "s.duckdb"), read_only=True)
    assert other.execute("select artist_mbid, count(*) from dim_artist group by 1 order by 1").fetchall() == [("A", 2), ("B", 1)]


def test_팩트의_NULL_아티스트는_차원_조건을_망가뜨리지_않는다(con, tmp_path):
    counts = sample.extract(con, SOURCES, {"bob": "x"}, tmp_path / "s.duckdb")
    assert counts["fact"] == 1
    assert counts["dim_artist"] == 0


def test_격리는_표본_Bronze_줄에서_온_것만_담는다(con, tmp_path):
    counts = sample.extract(con, SOURCES, {"alice": "x"}, tmp_path / "s.duckdb")
    assert counts["quarantine"] == 1


def test_사용자_표에_유형과_청취_수가_있다(con, tmp_path):
    sample.extract(con, SOURCES, {"alice": "가져오기 중심", "bob": "실시간 앱 중심"}, tmp_path / "s.duckdb")
    other = duckdb.connect(str(tmp_path / "s.duckdb"), read_only=True)
    assert other.execute("select user_name, kind, listens from sample_users order by user_name").fetchall() == [
        ("alice", "가져오기 중심", 2), ("bob", "실시간 앱 중심", 1)]


def test_Silver에_줄이_없는_사용자도_사용자_표에는_0건으로_남는다(con, tmp_path):
    sample.extract(con, SOURCES, {"alice": "x", "ghost": "y"}, tmp_path / "s.duckdb")
    other = duckdb.connect(str(tmp_path / "s.duckdb"), read_only=True)
    assert ("ghost", "y", 0) in other.execute("select * from sample_users").fetchall()


def test_다시_만들면_옛_표본을_지우고_새로_만든다(con, tmp_path):
    path = tmp_path / "s.duckdb"
    sample.extract(con, SOURCES, {"alice": "x"}, path)
    counts = sample.extract(con, SOURCES, {"bob": "x"}, path)
    assert counts["silver"] == 1
    other = duckdb.connect(str(path), read_only=True)
    assert other.execute("select distinct user_name from silver").fetchall() == [("bob",)]


def test_뽑은_사용자가_없으면_거부한다(con, tmp_path):
    with pytest.raises(ValueError):
        sample.extract(con, SOURCES, {}, tmp_path / "s.duckdb")


def test_원본_표가_모자라면_거부한다(con, tmp_path):
    with pytest.raises(ValueError):
        sample.extract(con, {"bronze": "src_bronze"}, {"alice": "x"}, tmp_path / "s.duckdb")


def test_원본_표는_바뀌지_않는다(con, tmp_path):
    before = con.execute("select count(*) from src_silver").fetchone()[0]
    sample.extract(con, SOURCES, {"alice": "x"}, tmp_path / "s.duckdb")
    assert con.execute("select count(*) from src_silver").fetchone()[0] == before


def test_사용자_통계가_유형_판정에_쓰는_값을_낸다(con):
    stats = {u.user_name: u for u in sample.read_user_stats(con, SOURCES, 1, 10)}
    alice = stats["alice"]
    assert alice.listens == 2
    assert alice.importer_share == 0.5
    assert alice.clients == 2
    assert alice.removed_duplicates == 3
    assert alice.collab_share == 0.5
    assert stats["bob"].collab_share == 0.0
    assert stats["carol"].collab_share == 0.0


def test_사용자_통계는_청취_범위_안만_돌려준다(con):
    assert {u.user_name for u in sample.read_user_stats(con, SOURCES, 2, 10)} == {"alice"}


def test_통계로_고른_사용자를_바로_추출할_수_있다(con, tmp_path):
    stats = sample.read_user_stats(con, SOURCES, 1, 10)
    chosen = pick.pick_users(stats, low=1, high=10)
    assert set(chosen) == {"alice", "bob", "carol"}
    assert sample.extract(con, SOURCES, chosen, tmp_path / "s.duckdb")["sample_users"] == 3
