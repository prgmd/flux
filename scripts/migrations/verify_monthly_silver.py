import argparse
import json
import time
from pathlib import Path

from flux.common import deltalog
from flux.common.drill_checkpoint import atomic_save

KEY = "user_name,recording_msid,listened_month"
VARIANT = "recording_msid,track_name,artist_name,recording_mbid,artist_mbids,mbid_source"


def compare_counts(c):
    c.execute(f"create or replace temp table expected as select {KEY},count(*)::bigint play_count from detail group by {KEY}")
    condition = " AND ".join(f"a.{k} IS NOT DISTINCT FROM e.{k}" for k in KEY.split(","))
    mismatches = c.execute(f"select count(*) from expected e full join actual a on {condition} where e.play_count is distinct from a.play_count").fetchone()[0]
    duplicates = c.execute(f"select count(*) from (select {KEY} from actual group by {KEY} having count(*)>1)").fetchone()[0]
    rows, plays, invalid = c.execute("select count(*),coalesce(sum(play_count),0),count(*) filter(where play_count is null or play_count<=0 or user_name is null or trim(user_name)='' or recording_msid is null or trim(recording_msid)='' or listened_month is null) from actual").fetchone()
    source_rows = c.execute("select count(*) from detail").fetchone()[0]
    return {"rows": rows, "plays": plays, "source_rows": source_rows,
            "mismatches": mismatches, "duplicate_keys": duplicates, "invalid_rows": invalid}


def compare_sets(c, expected, actual):
    return c.execute(f"select count(*) from (({expected} except all {actual}) union all ({actual} except all {expected}))").fetchone()[0]


def compare_metadata(c):
    c.execute(f"create or replace temp table expected_metadata as select distinct {VARIANT} from detail")
    try:
        return compare_sets(c, "select * from expected_metadata", f"select {VARIANT} from metadata")
    finally:
        c.execute("drop table expected_metadata")


def run(verified_root, output, *, threads=4):
    import duckdb
    from rebuild_playback import table_state

    verified_root, output = Path(verified_root), Path(output)
    manifest = json.loads((output / "migration.json").read_text(encoding="utf-8"))
    if not manifest["completed"]:
        raise RuntimeError("월별 생성이 끝나지 않았다")
    source_paths = {"bronze": verified_root / "bronze/raw_listens",
                    "silver": verified_root / "silver/listens_normalized",
                    "quarantine": verified_root / "silver/invalid_listens"}
    for name, path in source_paths.items():
        if table_state(path) != manifest["sources"][name]["state"]:
            raise RuntimeError("원본 버전이 달라졌다")
    targets = {name: output / name for name in
               ("monthly_user_recordings", "recording_metadata", "invalid_listens")}
    target_before = {name: table_state(path) for name, path in targets.items()}
    c = duckdb.connect()
    c.execute(f"set threads={int(threads)}")
    c.execute("set memory_limit='8GB'")
    c.execute("set TimeZone='UTC'")
    c.execute("set temp_directory='" + (output / "verification-tmp").as_posix().replace("'", "''") + "'")
    tables = {"detail": source_paths["silver"], "actual": targets["monthly_user_recordings"],
              "metadata": targets["recording_metadata"], "quarantine": targets["invalid_listens"],
              "old_quarantine": source_paths["quarantine"]}
    for name, path in tables.items():
        c.read_parquet(deltalog.active_paths(path), hive_partitioning=True).create_view(name)
    started = time.monotonic()
    print("start: monthly key comparison", flush=True)
    result = {"counts": compare_counts(c)}
    print("done: counts", result["counts"], flush=True)
    c.execute("drop table expected")
    print("start: metadata variants comparison", flush=True)
    result["metadata_mismatches"] = compare_metadata(c)
    print("done: metadata", result["metadata_mismatches"], flush=True)
    print("start: quarantine comparison", flush=True)
    qcols = ",".join('"' + row[0].replace('"', '""') + '"'
                     for row in c.execute("describe old_quarantine").fetchall())
    result["quarantine_mismatches"] = compare_sets(c,
        f"select {qcols} from old_quarantine", f"select {qcols} from quarantine")
    print("done: quarantine", result["quarantine_mismatches"], flush=True)
    result["metadata_rows"] = c.execute("select count(*) from metadata").fetchone()[0]
    result["quarantine_rows"] = c.execute("select count(*) from quarantine").fetchone()[0]
    result["physical"] = {name: {"files": len(deltalog.active_paths(path)),
        "bytes": sum(Path(p).stat().st_size for p in deltalog.active_paths(path))}
        for name, path in targets.items()}
    checkpoints = []
    for path in targets.values():
        last = list(deltalog.actions(deltalog.commits(path)[-1]))
        info = next(a["commitInfo"] for a in last if "commitInfo" in a)
        checkpoints.append(json.loads(info["userMetadata"])["monthly_silver"])
    result["checkpoint_match"] = all(s == checkpoints[0] for s in checkpoints)
    result["bronze_checkpoint_match"] = (checkpoints[0]["source_id"] == manifest["sources"]["bronze"]["state"]["id"]
        and checkpoints[0]["version"] == manifest["sources"]["bronze"]["state"]["version"])
    result["policy_match"] = all(s.get("key") == KEY.split(",")
        and s.get("minimum_ms_played") == 30000 and s.get("timezone") == "UTC"
        and s.get("policy_version") == 1 for s in checkpoints)
    result["cdf_enabled"] = all(s["properties"].get("delta.enableChangeDataFeed") == "true"
                                for s in target_before.values())
    result["source_unchanged"] = all(table_state(path) == manifest["sources"][name]["state"]
                                      for name, path in source_paths.items())
    result["target_unchanged"] = all(table_state(path) == target_before[name] for name, path in targets.items())
    counts = result["counts"]
    result["ok"] = (counts["plays"] == counts["source_rows"]
        and not any(counts[k] for k in ("mismatches", "duplicate_keys", "invalid_rows"))
        and result["metadata_mismatches"] == result["quarantine_mismatches"] == 0
        and all(result[k] for k in ("checkpoint_match", "bronze_checkpoint_match", "policy_match", "cdf_enabled",
                                    "source_unchanged", "target_unchanged")))
    result["seconds"] = round(time.monotonic() - started, 2)
    atomic_save(output / "verification.json", result)
    c.close()
    if not result["ok"]:
        raise RuntimeError("월별 Silver 전수 대조 실패")
    manifest["independent_validation_pending"] = False
    atomic_save(output / "migration.json", manifest)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verified-root", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--threads", type=int, default=4)
    args = parser.parse_args()
    print(json.dumps(run(args.verified_root, args.output, threads=args.threads)), flush=True)
