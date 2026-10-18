from pathlib import Path

import duckdb
import importlib.util


def test_repeats_listeners_ties_and_artist_identity():
    con = duckdb.connect()
    con.execute("create table fact(artist_mbid varchar, user_id varchar)")
    con.executemany("insert into fact values (?, ?)",
                    [("a", "one")] * 10 + [("b", str(i)) for i in range(3)]
                    + [("c", "one"), (None, "one")])
    con.execute("create table dim_artist(artist_mbid varchar, name varchar, is_current boolean)")
    con.executemany("insert into dim_artist values (?, ?, true)",
                    [("a", "same"), ("b", "same"), ("c", "other")])
    sql = (Path(__file__).resolve().parents[1] / "scripts/investigations/sql/artist_rank_comparison.sql").read_text()
    rows = con.sql(sql.replace("{fact}", "fact").replace("{dim_artist}", "dim_artist")).fetchall()
    result = {r[0]: r for r in rows}
    assert len(result) == 3
    assert result["a"][2:7] == (10, 1, 1, 2, 10)
    assert result["b"][2:7] == (3, 3, 2, 1, 1)
    assert result["c"][5] == 2
    assert result["a"][7:] == (100.0, 10.0)
    assert result["b"][7:] == (33.33, 1.0)


def test_source_comparison_catches_wrong_counts_and_missing_pairs():
    path = Path(__file__).resolve().parents[1] / "scripts/investigations/compare_artist_rankings.py"
    spec = importlib.util.spec_from_file_location("rank_analysis", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    con = duckdb.connect()
    con.execute("create table silver(user_id varchar, artist_mbids varchar[])")
    con.execute("insert into silver values ('u', ['a', 'a', 'b']), ('u', []), ('v', NULL)")
    con.execute("create table fact(user_id varchar, artist_mbid varchar)")
    con.execute("insert into fact values ('u', 'a'), ('u', 'b'), ('v', NULL)")
    assert module.verify_source(con) == 0
    con.execute("insert into fact values ('u', 'a'), ('new', 'b')")
    assert module.verify_source(con) == 2
