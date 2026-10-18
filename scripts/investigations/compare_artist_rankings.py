import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from flux.common import deltalog


def head(table):
    return max(int(p.stem) for p in (table / "_delta_log").glob("*.json") if p.stem.isdigit())


def verify_source(con):
    return con.execute("""
        with expanded as (
            select user_id, unnest(list_distinct(artist_mbids)) as artist_mbid
            from silver
        ), expected as (
            select user_id, artist_mbid, count(*) as n
            from expanded
            group by user_id, artist_mbid
        ), actual as (
            select user_id, artist_mbid, count(*) as n
            from fact where artist_mbid is not null
            group by user_id, artist_mbid
        )
        select count(*) from expected e full outer join actual a
        using (user_id, artist_mbid)
        where e.n is distinct from a.n
    """).fetchone()[0]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("set memory_limit='10GB'")
    con.execute("set threads=4")
    con.execute("set preserve_insertion_order=false")
    con.execute("set TimeZone='UTC'")
    con.execute(f"set temp_directory='{(args.output / 'tmp').as_posix()}'")
    snapshots = {}
    tables = {"silver": "silver/listens_normalized", "fact": "gold/fact_listens",
              "dim_artist": "gold/dim_artist"}
    started = time.monotonic()
    for name, relative in tables.items():
        table = args.data_root / relative
        before = head(table)
        files = deltalog.active_paths(table)
        if head(table) != before:
            raise RuntimeError(f"{name}: snapshot changed")
        snapshots[name] = {"table": str(table), "version": before, "files": files}
        con.read_parquet(files, hive_partitioning=True).create_view(name)
    (args.output / "snapshots.json").write_text(json.dumps(snapshots), encoding="utf-8")
    sql = (Path(__file__).parent / "sql/artist_rank_comparison.sql").read_text(encoding="utf-8")
    for name in tables:
        sql = sql.replace("{" + name + "}", name)
    result = con.execute(sql)
    columns = [c[0] for c in result.description]
    artists = [dict(zip(columns, r)) for r in result.fetchall()]
    print(f"ranked {len(artists)} artists in {time.monotonic() - started:.1f}s", flush=True)
    (args.output / "artists.json").write_text(json.dumps(artists, ensure_ascii=False), encoding="utf-8")
    result = con.execute("""
        select count(*) as silver_listens, count(distinct user_id) as silver_users,
               min(listened_at) as first_listen, max(listened_at) as last_listen
        from silver
    """)
    coverage = dict(zip([c[0] for c in result.description], result.fetchone()))
    result = con.execute("""
        select count(*) as fact_rows,
               count(distinct (_source_dump, _record_id)) as fact_listens,
               count(distinct (_source_dump, _record_id)) filter (where artist_mbid is not null) as linked_listens,
               count(distinct user_id) filter (where artist_mbid is not null) as linked_users
        from fact
    """)
    coverage.update(zip([c[0] for c in result.description], result.fetchone()))
    source_mismatches = verify_source(con)
    if source_mismatches:
        raise RuntimeError(f"Silver/fact artist-user mismatches: {source_mismatches}")
    for name, snapshot in snapshots.items():
        if head(Path(snapshot["table"])) != snapshot["version"]:
            raise RuntimeError(f"{name}: changed during analysis")
    output = {"versions": {k: v["version"] for k, v in snapshots.items()},
              "coverage": coverage, "seconds": round(time.monotonic() - started, 1),
              "source_pair_mismatches": source_mismatches,
              "top_plays": artists[:20],
              "top_listeners": sorted(artists, key=lambda a: (a["listener_rank"], a["artist_mbid"]))[:20],
              "selected": [a for a in artists if a["artist"] in
                           {"RM", "PNL", "BTS", "The Beatles", "Radiohead", "Taylor Swift", "Linkin Park"}]}
    (args.output / "summary.json").write_text(json.dumps(output, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print(json.dumps(output, ensure_ascii=True, default=str), flush=True)


if __name__ == "__main__":
    main()
