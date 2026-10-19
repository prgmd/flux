import argparse
import hashlib
import json
import time
from pathlib import Path

from flux.common import deltalog


def prepare_mapping(c, policy):
    if policy == "mbid":
        query = """select recording_msid,min(nullif(trim(recording_mbid),'')) song_key
            from metadata group by recording_msid
            having count(distinct nullif(trim(recording_mbid),''))=1"""
    elif policy == "name":
        query = """select recording_msid,min(to_json(struct_pack(
                artist:=lower(trim(artist_name)),track:=lower(trim(track_name))))) song_key
            from metadata group by recording_msid
            having count(*) filter(where artist_name is null or trim(artist_name)=''
                or track_name is null or trim(track_name)='')=0
            and count(distinct (lower(trim(artist_name)),lower(trim(track_name))))=1"""
    else:
        raise ValueError(policy)
    c.execute(f"""create or replace temp table mapping as
        with consistent as ({query}), multi as (
            select song_key from consistent group by song_key having count(*)>1)
        select consistent.* from consistent join multi using(song_key)""")


def measure_policy(c):
    for name in ("mapped_months", "event_groups", "detail_months", "duplicate_events", "affected_months"):
        c.execute(f"drop table if exists {name}")
    c.execute("""create or replace temp table dictionary as select song_key external_key,
        row_number() over() song_key from (select distinct song_key from mapping)""")
    c.execute("""create or replace temp table working_mapping as select recording_msid,d.song_key
        from mapping m join dictionary d on m.song_key=d.external_key""")
    rows, plays = c.execute("select count(*),sum(play_count) from monthly").fetchone()
    c.execute("""create temp table mapped_months as select user_name,song_key,listened_month,
        sum(play_count)::bigint plays_before,count(*) source_keys
        from monthly join working_mapping using(recording_msid) group by all""")
    mapped_keys, merged_keys = c.execute("select coalesce(sum(source_keys),0),count(*) from mapped_months").fetchone()
    print("monthly merge measured", flush=True)
    c.execute("""create temp table event_groups as select user_name,song_key,listened_at,listened_month,
        count(*)::bigint n
        from detail join working_mapping using(recording_msid) group by all""")
    print("event groups measured", flush=True)
    c.execute("""create temp table duplicate_events as
        select *,n msids from event_groups where n>1""")
    invalid = c.execute("""select count(*) from (
        select e.user_name,e.song_key,e.listened_at,e.listened_month,e.n,
            count(distinct d.recording_msid) distinct_msids
        from detail d join working_mapping m using(recording_msid)
        join duplicate_events e on d.user_name=e.user_name and d.listened_at=e.listened_at
            and d.listened_month=e.listened_month and m.song_key=e.song_key
        group by all having n<>distinct_msids)""").fetchone()[0]
    if invalid:
        raise ValueError("detail has duplicate user/MSID/timestamp keys")
    c.execute("""create temp table detail_months as select user_name,song_key,listened_month,
        sum(n)::bigint plays_before from event_groups group by all""")
    mismatches = c.execute("""select count(*) from mapped_months m full join detail_months d
        using(user_name,song_key,listened_month) where m.plays_before is distinct from d.plays_before""").fetchone()[0]
    if mismatches:
        raise ValueError(f"monthly/detail mismatches: {mismatches}")
    print("monthly/detail comparison passed", flush=True)
    c.execute("drop table event_groups")
    c.execute("drop table detail_months")
    c.execute("""create temp table affected_months as select user_name,song_key,listened_month,
        plays_before,extra_rows from mapped_months join (
            select user_name,song_key,listened_month,sum(n-1)::bigint extra_rows
            from duplicate_events group by all) using(user_name,song_key,listened_month)""")
    groups, extra, max_msids = c.execute("select count(*),coalesce(sum(n-1),0),coalesce(max(msids),0) from duplicate_events").fetchone()
    return {
        "monthly_rows_before": rows, "plays_before": plays,
        "candidate_msids": c.execute("select count(*) from mapping").fetchone()[0],
        "candidate_song_groups": c.execute("select count(distinct song_key) from mapping").fetchone()[0],
        "mapped_monthly_rows": mapped_keys,
        "monthly_rows_after_merge": rows - mapped_keys + merged_keys,
        "plays_after_merge": plays, "monthly_detail_mismatches": mismatches,
        "duplicate_event_groups": groups, "extra_event_rows": extra,
        "max_msids_per_event": max_msids,
        "plays_after_event_rule": plays - extra,
        "affected_user_song_months": c.execute("select count(*) from affected_months").fetchone()[0],
    }


def records(c, sql):
    rows = c.execute(sql).fetchall()
    return [dict(zip([d[0] for d in c.description], row)) for row in rows]


def evidence(c):
    c.execute("""create or replace temp table top_songs as select song_key,sum(extra_rows)::bigint extra_rows,
        sum(plays_before)::bigint affected_plays_before,count(*) affected_months
        from affected_months group by song_key order by extra_rows desc,song_key limit 12""")
    result = {"top_songs": records(c, """select d.external_key song_key,t.* exclude(song_key)
        from top_songs t join dictionary d using(song_key) order by extra_rows desc,song_key""")}
    result["variants"] = records(c, """select distinct d.external_key song_key,metadata.*
        from metadata join working_mapping using(recording_msid) join top_songs using(song_key)
        join dictionary d using(song_key) order by song_key,recording_msid limit 160""")
    c.execute("""create or replace temp table sample_events as select * from duplicate_events
        order by msids desc,song_key,listened_at,user_name limit 12""")
    result["event_examples"] = records(c, """select dense_rank() over(order by e.song_key,e.listened_at,e.user_name) sample_number,
        k.external_key song_key,cast(e.listened_at as varchar) listened_at,e.n,d.recording_msid,
        d.recording_mbid,d.track_name,d.artist_name,d.client_name,d._source_dump
        from detail d join working_mapping m using(recording_msid) join sample_events e
        on d.user_name=e.user_name and d.listened_at=e.listened_at and m.song_key=e.song_key
        join dictionary k on e.song_key=k.song_key
        order by sample_number,d.recording_msid""")
    result["affected_month_examples"] = records(c, """select d.external_key song_key,
        listened_month,plays_before,extra_rows,plays_before-extra_rows plays_after
        from affected_months join dictionary d using(song_key)
        order by extra_rows desc,d.external_key,listened_month,user_name limit 12""")
    return result


def fingerprint(path):
    logs = deltalog.commits(path)
    return {"path": str(path.resolve()), "version": int(logs[-1].stem),
            "log_sha256": hashlib.sha256(b"".join(p.read_bytes() for p in logs)).hexdigest()}


def run(monthly_root, detail_path, output, policies, threads=2, memory_gb=16):
    import duckdb

    paths = {"monthly": monthly_root / "monthly_user_recordings",
             "metadata": monthly_root / "recording_metadata", "detail": detail_path}
    output.mkdir(parents=True, exist_ok=False)
    before = {name: fingerprint(path) for name, path in paths.items()}
    c = duckdb.connect(str(output / "audit.duckdb"))
    c.execute(f"set threads={int(threads)}")
    c.execute(f"set memory_limit='{int(memory_gb)}GB'")
    c.execute("set preserve_insertion_order=false")
    c.execute("set TimeZone='UTC'")
    c.execute("set max_temp_directory_size='60GB'")
    for name, path in paths.items():
        c.read_parquet(deltalog.active_paths(path), hive_partitioning=True).create_view(name)
    result = {"source_snapshots": before, "hypothetical_only": True, "policies": {},
              "completed": False, "threads": threads, "memory_gb": memory_gb}
    (output / "snapshots.json").write_text(json.dumps(before, indent=2), encoding="utf-8")
    started = time.monotonic()
    for policy in policies:
        print(f"start {policy}", flush=True)
        tick = time.monotonic()
        prepare_mapping(c, policy)
        print(f"mapping ready {policy}", flush=True)
        measured = measure_policy(c)
        measured["evidence"] = evidence(c)
        measured["seconds"] = round(time.monotonic() - tick, 2)
        result["policies"][policy] = measured
        (output / f"{policy}.json").write_text(json.dumps(measured, ensure_ascii=False, indent=2), encoding="utf-8")
        (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print(policy, json.dumps({k: v for k, v in measured.items() if k != "evidence"}), flush=True)
    result["source_unchanged"] = before == {name: fingerprint(path) for name, path in paths.items()}
    result["seconds"] = round(time.monotonic() - started, 2)
    if not result["source_unchanged"]:
        raise ValueError("source changed during audit")
    result["completed"] = True
    (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    c.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--monthly-root", type=Path, required=True)
    parser.add_argument("--detail", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--policies", nargs="+", choices=("mbid", "name"), default=["mbid", "name"])
    parser.add_argument("--threads", type=int, default=2)
    parser.add_argument("--memory-gb", type=int, default=16)
    args = parser.parse_args()
    inputs = [args.monthly_root.resolve(), args.detail.resolve()]
    target = args.output.resolve()
    if any(target == p or target in p.parents or p in target.parents for p in inputs):
        raise ValueError("output overlaps input")
    run(args.monthly_root, args.detail, args.output, args.policies, args.threads, args.memory_gb)


if __name__ == "__main__":
    main()
