import argparse
import json
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from flux.common import deltalog
from flux.common.drill_checkpoint import atomic_save
from flux.silver.quality import PLAYED_NUMBER_PATTERN
from rebuild_playback import identity, table_state


def read_table(con, name, table, files):
    if files:
        con.read_parquet(files, hive_partitioning=True, filename=True).create_view(name, replace=True)
        return
    metadata = None
    for log in deltalog.commits(table):
        for action in deltalog.actions(log):
            if "metaData" in action:
                metadata = action["metaData"]

    def sql_type(value):
        if isinstance(value, dict) and value["type"] == "array":
            return sql_type(value["elementType"]) + "[]"
        return {"string": "varchar", "long": "bigint", "integer": "integer", "short": "smallint",
                "date": "date", "timestamp": "timestamp", "double": "double", "float": "float",
                "boolean": "boolean", "binary": "blob"}[value]

    fields = json.loads(metadata["schemaString"])["fields"]
    columns = [f'cast(null as {sql_type(f["type"])}) as "{f["name"]}"' for f in fields]
    con.execute(f'create or replace view "{name}" as select ' + ", ".join(columns)
                + ", cast(null as varchar) as filename where false")


def prepare(con):
    pattern = PLAYED_NUMBER_PATTERN.replace(r"\v", r"\x0b")
    con.execute("""
        create or replace macro uuid_value(v) as
          case when regexp_full_match(trim(v), '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}')
          then lower(trim(v)) end;
        create or replace macro artist_values(v) as
          nullif(list_filter(list_transform(
            case when json_type(try_cast(v as json)) = 'ARRAY' then try_cast(try_cast(v as json) as varchar[])
                 when v is not null then [v] end,
            x -> uuid_value(x)), x -> x is not null), []);
    """)
    con.execute("""
        create or replace table judged as
        with extracted as (
          select _source_dump, _record_id, user_name, listened_at, _ingested_at,
            json_extract_string(raw_json, [
              '$.user_id', '$.recording_msid', '$.track_metadata.additional_info.ms_played',
              '$.track_metadata.mbid_mapping.recording_mbid',
              '$.track_metadata.additional_info.recording_mbid',
              '$.track_metadata.additional_info.lastfm_track_mbid',
              '$.track_metadata.mbid_mapping.artist_mbids',
              '$.track_metadata.additional_info.artist_mbids',
              '$.track_metadata.additional_info.lastfm_artist_mbid']) as fields,
            json_type(raw_json, '$.track_metadata') as metadata_type
          from bronze
        )
        select *, case
          when trim(coalesce(user_name, '')) = '' then '사용자 이름 없음'
          when listened_at is null then '청취 시각 없음'
          when listened_at < 0 then '청취 시각이 1970 이전'
          when listened_at > epoch(_ingested_at) then '청취 시각이 적재 시각보다 미래'
          when metadata_type is distinct from 'OBJECT' then 'track_metadata 없음'
          when regexp_full_match(fields[3], ?) and try_cast(fields[3] as double) < 30000
            then '재생 시간이 30초 미만'
        end as reason
        from extracted
    """, [pattern])
    con.execute("""
        create or replace table expected_silver as
        with parsed as (
          select _source_dump, _record_id, user_name, listened_at,
            fields[1] as user_id, fields[2] as recording_msid,
            uuid_value(fields[4]) as mapping, uuid_value(fields[5]) as client,
            uuid_value(fields[6]) as lastfm,
            artist_values(fields[7]) as mapping_artists,
            artist_values(fields[8]) as client_artists,
            artist_values(fields[9]) as lastfm_artists
          from judged where reason is null
        ), resolved as (
          select *, coalesce(mapping, client, lastfm) as recording_mbid,
            case when mapping is not null then 'MAPPING' when client is not null then 'CLIENT'
                 when lastfm is not null then 'LASTFM' else 'NONE' end as mbid_source,
            case when mapping is not null then mapping_artists
                 when client is not null then client_artists
                 when lastfm is not null then lastfm_artists
                 else coalesce(mapping_artists, client_artists, lastfm_artists) end as artist_mbids
          from parsed
        )
        select _source_dump, _record_id, user_name, listened_at, user_id, recording_msid,
               recording_mbid, mbid_source, artist_mbids
        from resolved
        qualify row_number() over (
          partition by user_name, listened_at, recording_msid
          order by case mbid_source when 'MAPPING' then 0 when 'CLIENT' then 1 when 'LASTFM' then 2 else 3 end,
                   coalesce(len(artist_mbids), 0) desc, _source_dump desc, _record_id
        ) = 1
    """)


def compare(con, expected, actual, keys, columns):
    join = " and ".join(f'e."{c}" is not distinct from a."{c}"' for c in keys)
    different = " or ".join(f'e."{c}" is distinct from a."{c}"' for c in columns)
    sql = f"""select count(*) from ({expected}) e full outer join ({actual}) a on {join}
              where e."{keys[0]}" is null or a."{keys[0]}" is null or ({different})"""
    mismatches = con.execute(sql).fetchone()[0]
    grouped = ", ".join('"' + c + '"' for c in keys)
    duplicates = con.execute(f"select count(*) from (select {grouped} from ({actual}) group by {grouped} having count(*) > 1)").fetchone()[0]
    if mismatches or duplicates:
        raise AssertionError({"mismatches": mismatches, "duplicate_keys": duplicates})
    return {"mismatches": mismatches, "duplicate_keys": duplicates}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--production-root", type=Path, required=True)
    parser.add_argument("--rebuild-root", type=Path, required=True)
    parser.add_argument("--prepare-only", action="store_true")
    args = parser.parse_args()
    root = args.rebuild_root
    state = json.loads((root / "rebuild.json").read_text(encoding="utf-8"))
    before = identity(args.production_root)
    if before != state["production_before"]:
        raise RuntimeError("Production baseline changed")
    con = duckdb.connect(str(root / "oracle.duckdb"))
    con.execute("set threads=4")
    con.execute("set memory_limit='8GB'")
    con.execute("set preserve_insertion_order=false")
    con.execute("set TimeZone='UTC'")
    con.execute(f"set temp_directory='{(root / 'oracle-tmp').as_posix()}'")
    snapshots = {}

    def view(name, path):
        initial = table_state(path)
        files = deltalog.active_paths(path)
        if initial != table_state(path):
            raise RuntimeError("Snapshot changed: " + name)
        read_table(con, name, path, files)
        snapshots[name] = {"path": str(path), "state": initial, "files": files}

    view("bronze", args.production_root / "bronze/raw_listens")
    report_path = root / "verification.json"
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {"checks": {}}
    report.pop("ok", None)
    report.pop("production_unchanged", None)
    atomic_save(report_path, report)
    if "expected_silver" not in {r[0] for r in con.execute("show tables").fetchall()}:
        started = time.monotonic()
        prepare(con)
        report["preparation_seconds"] = round(time.monotonic() - started, 1)
    report["quality"] = dict(con.execute("select coalesce(reason, 'PASS'), count(*) from judged group by reason").fetchall())
    report["expected_silver_rows"] = con.execute("select count(*) from expected_silver").fetchone()[0]
    atomic_save(report_path, report)
    print("독립 품질 판정과 예상 승자 계산 완료", json.dumps({k: v for k, v in report.items() if k != "checks"}, ensure_ascii=False), flush=True)
    if args.prepare_only:
        return
    if not state.get("completed"):
        raise RuntimeError("Rebuild stages are not complete")
    tables = {"silver": "silver/listens_normalized", "quarantine": "silver/quarantined_listens",
              "counts": "silver/user_recording_counts", "fact": "gold/fact_listens",
              "dim": "gold/dim_artist", "daily": "gold/mart_daily_artist",
              "rising": "gold/mart_rising_artist", "periods": "gold/mart_period_artist",
              "users": "gold/dim_user", "calendar": "gold/dim_date"}
    from flux.silver import schema
    tables["quarantine"] = "silver/" + schema.QUARANTINE_TABLE_NAME
    for name, relative in tables.items():
        view(name, root / relative)
    atomic_save(root / "verification-snapshots.json", snapshots)

    def check(name, operation):
        started = time.monotonic()
        report["checks"][name] = operation()
        report["checks"][name]["seconds"] = round(time.monotonic() - started, 1)
        atomic_save(report_path, report)
        print(name, json.dumps(report["checks"][name], ensure_ascii=False), flush=True)

    check("quarantine", lambda: compare(con, "select _source_dump, _record_id, reason from judged where reason is not null",
          "select * from quarantine", ["_source_dump", "_record_id"], ["reason"]))
    check("silver", lambda: compare(con, "select * exclude(listened_at), listened_at as listened_epoch from expected_silver",
          "select *, epoch(listened_at)::bigint as listened_epoch from silver",
          ["_source_dump", "_record_id"], ["user_name", "user_id", "recording_msid", "recording_mbid", "mbid_source", "artist_mbids", "listened_epoch"]))
    check("counts", lambda: compare(con, "select user_id, recording_msid, count(*) as play_count from silver group by user_id, recording_msid",
          "select * from counts", ["user_id", "recording_msid"], ["play_count"]))
    check("fact", lambda: compare(con, """select _source_dump, _record_id, user_id, listened_at, recording_mbid,
          release_mbid, mbid_source, client_name, duration_ms, listened_month,
          unnest(case when coalesce(len(artist_mbids), 0)=0 then [NULL] else list_distinct(artist_mbids) end) as artist_mbid
          from silver""", "select * from fact", ["_source_dump", "_record_id", "artist_mbid"],
          ["user_id", "listened_at", "recording_mbid", "release_mbid", "mbid_source", "client_name", "duration_ms", "listened_month"]))
    check("daily", lambda: compare(con, """with totals as (
          select artist_mbid, cast(listened_at as date) as listened_date, count(*) as listens,
                 count(distinct (_source_dump, _record_id)) as distinct_listens
          from fact where artist_mbid is not null group by artist_mbid, listened_date)
          select t.*, d.name as artist_name from totals t left join dim d on t.artist_mbid=d.artist_mbid
          and t.listened_date >= cast(d.valid_from as date) and t.listened_date < cast(d.valid_to as date)""",
          "select * from daily", ["artist_mbid", "listened_date"], ["listens", "distinct_listens", "artist_name"]))
    check("rising", lambda: compare(con, """with weeks as (
          select artist_mbid, cast(date_trunc('week', listened_date) as date) as week_start,
                 sum(listens) as listens, arg_max(artist_name, listened_date) as artist_name
          from daily group by artist_mbid, week_start), ranked as (
          select w.*, p.listens as previous_listens, w.listens*1.0/p.listens as growth,
                 row_number() over(partition by w.week_start order by w.listens*1.0/p.listens desc, w.listens desc, w.artist_mbid) as rank
          from weeks w join weeks p on w.artist_mbid=p.artist_mbid and w.week_start=p.week_start+7
          where p.listens>=20)
          select * exclude(growth), round(growth,12) as growth from ranked where rank<=20""",
          "select * exclude(growth), round(growth,12) as growth from rising", ["week_start", "artist_mbid"],
          ["rank", "listens", "previous_listens", "growth", "artist_name"]))
    check("periods", lambda: verify_periods(con))
    check("users", lambda: compare(con, """select user_id, arg_max(user_name, (listened_at,user_name)) as user_name,
          min(listened_at) as first_listened_at, max(listened_at) as last_listened_at, count(*) as listens
          from silver group by user_id""", "select * from users", ["user_id"],
          ["user_name", "first_listened_at", "last_listened_at", "listens"]))
    check("calendar", lambda: compare(con, """with span as (
          select min(cast(listened_at as date)) as first, max(cast(listened_at as date)) as last from silver),
          days as (select cast(unnest(generate_series(first,last,interval 1 day)) as date) as date from span)
          select date, year(date) as year, month(date) as month, day(date) as day,
                 strftime(date,'%Y-%m') as listened_month, isoyear(date) as iso_year,
                 week(date) as iso_week, isodow(date) as day_of_week, isodow(date)>=6 as is_weekend,
                 cast(date_trunc('week',date) as date) as week_start from days""", "select * from calendar",
          ["date"], ["year", "month", "day", "listened_month", "iso_year", "iso_week", "day_of_week", "is_weekend", "week_start"]))
    check("integrity", lambda: integrity(con, snapshots))
    for name, snapshot in snapshots.items():
        if table_state(Path(snapshot["path"])) != snapshot["state"]:
            raise RuntimeError("Verified table changed: " + name)
    if identity(args.production_root) != before:
        raise RuntimeError("Production changed during verification")
    report["ok"] = True
    report["production_unchanged"] = True
    atomic_save(report_path, report)
    print("전체 독립 대조 통과", flush=True)


def verify_periods(con):
    con.execute("""create or replace temp table period_pairs as
      select period, cast(date_trunc(period, listened_at) as date) as period_start,
             artist_mbid, user_id, count(*) as n
      from fact cross join (values ('day'), ('week')) p(period)
      where artist_mbid is not null group by period, period_start, artist_mbid, user_id""")
    return compare(con, """with metrics as (
      select period, period_start, artist_mbid, sum(n) as listens, count(*) as listeners,
             max(n) as top_user_listens from period_pairs group by period, period_start, artist_mbid
      ), coverage as (
      select period, cast(date_trunc(period,listened_at) as date) as period_start,
             count(*) as period_listens, count(*) filter(where len(artist_mbids)>0) as linked_listens,
             count(distinct user_id) as period_users,
             count(distinct user_id) filter(where len(artist_mbids)>0) as linked_users
      from silver cross join (values ('day'), ('week')) p(period) group by period, period_start
      ), ranked as (
      select *, row_number() over(partition by period, period_start order by listeners desc, listens desc, artist_mbid) as rank
      from metrics)
      select *, period_start + case when period='day' then 1 else 7 end as period_end,
             round(top_user_listens*1.0/listens,12) as top_user_share,
             round(linked_listens*1.0/period_listens,12) as coverage
      from ranked join coverage using(period,period_start)""",
      "select * exclude(top_user_share,coverage), round(top_user_share,12) as top_user_share, round(coverage,12) as coverage from periods",
      ["period", "period_start", "artist_mbid"],
      ["period_end", "listens", "listeners", "rank", "top_user_listens", "top_user_share", "period_listens", "linked_listens", "period_users", "linked_users", "coverage"])


def integrity(con, snapshots):
    checks = {
      "duplicate_silver_keys": "select count(*) from (select user_name,listened_at,recording_msid from silver group by all having count(*)>1)",
      "null_keys": "select count(*) from silver where user_id is null or recording_msid is null or user_name is null or listened_at is null",
      "missing_artists": "select count(*) from (select distinct artist_mbid from fact where artist_mbid is not null except select artist_mbid from dim)",
      "missing_dates": "select count(*) from (select distinct cast(listened_at as date) from fact except select date from calendar)",
      "split_user_months": "select count(*) from (select user_name,listened_month from silver group by all having count(distinct filename)>1)",
    }
    results = {name: con.execute(sql).fetchone()[0] for name, sql in checks.items()}
    results["cdf_off"] = int(snapshots["silver"]["state"]["properties"].get("delta.enableChangeDataFeed") != "true")
    metadata = {}
    for name in ("counts", "fact", "periods"):
        log = deltalog.commits(Path(snapshots[name]["path"]))[-1]
        metadata[name] = next(a["commitInfo"].get("userMetadata") for a in deltalog.actions(log) if "commitInfo" in a)
    counts_state = json.loads(metadata["counts"])["user_recording_counts"]
    results["counts_wrong_source"] = int(counts_state["source_id"] != snapshots["silver"]["state"]["id"]
                                         or counts_state["version"] != snapshots["silver"]["state"]["version"])
    from flux.gold.fact import parse_watermark
    results["fact_wrong_version"] = int(parse_watermark(metadata["fact"]) != snapshots["silver"]["state"]["version"])
    periods_state = json.loads(metadata["periods"])
    results["periods_wrong_source"] = int(periods_state["fact_id"] != snapshots["fact"]["state"]["id"]
                                          or periods_state["fact_version"] != snapshots["fact"]["state"]["version"])
    if any(results.values()):
        raise AssertionError(results)
    return results


if __name__ == "__main__":
    main()
