import argparse
import hashlib
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

from flux.common import deltalog
from flux.common.drill_checkpoint import atomic_save


def separate(first, second):
    first, second = Path(first).resolve(), Path(second).resolve()
    if first == second or first in second.parents or second in first.parents:
        raise ValueError("Rebuild and production roots must be separate")


def table_state(table):
    logs = deltalog.commits(table)
    metadata = None
    digest = hashlib.sha256()
    for path in logs:
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
        for action in deltalog.actions(path):
            if "metaData" in action:
                metadata = action["metaData"]
    return {"id": metadata["id"], "version": int(logs[-1].stem),
            "log_sha256": digest.hexdigest(), "properties": metadata.get("configuration", {})}


def identity(root):
    return {str(table.relative_to(root)).replace("\\", "/"): table_state(table)
            for layer in ("bronze", "silver", "gold")
            for table in sorted((Path(root) / layer).glob("*"))
            if (table / "_delta_log").is_dir()}


def run(spark, production, work, export, *, silver_only=False):
    from flux.gold import artist_periods_build, dates, dim_date, dim_user, fact, fact_listens, mart, mart_build
    from flux.silver import build, schema, user_recording_counts

    production, work, export = Path(production), Path(work), Path(export)
    separate(production, work)
    separate(production, export)
    separate(work, export)
    export.mkdir(parents=True, exist_ok=True)
    state_path = work / "rebuild.json"
    if state_path.exists():
        state = json.loads(state_path.read_text(encoding="utf-8"))
        if state["production_before"] != identity(production):
            raise RuntimeError("Production changed since the recorded baseline")
    else:
        state = {"started_at": datetime.now(timezone.utc).isoformat(), "minimum_ms_played": 30000,
                 "production_before": identity(production), "stages": {}}
        atomic_save(state_path, state)

    if "silver_only" in state and state["silver_only"] != silver_only:
        raise RuntimeError("Rebuild scope changed")
    state["silver_only"] = silver_only

    def path(relative):
        return (work / relative).as_uri()

    def load(relative):
        return spark.read.format("delta").load(path(relative))

    def persist():
        atomic_save(state_path, state)
        atomic_save(export / "rebuild.json", state)

    def stage(name, tables, action):
        previous = state["stages"].get(name)
        if previous and previous.get("completed"):
            if previous["tables"] != {p: table_state(work / p) for p in tables}:
                raise RuntimeError("Completed stage changed: " + name)
            print("resume: " + name, flush=True)
        else:
            started = time.monotonic()
            state["active_stage"] = name
            persist()
            print("start: " + name, flush=True)
            action()
            state["stages"][name] = {"completed": True, "seconds": round(time.monotonic() - started, 1),
                                     "tables": {p: table_state(work / p) for p in tables}}
            persist()
            print("completed: " + name, json.dumps(state["stages"][name]), flush=True)
        for relative in tables:
            shutil.copytree(work / relative, export / relative, dirs_exist_ok=True)
        spark.catalog.clearCache()

    silver = "silver/listens_normalized"
    quarantine = "silver/" + schema.QUARANTINE_TABLE_NAME
    counts = "silver/" + user_recording_counts.TABLE_NAME
    artist = "gold/dim_artist"
    facts = "gold/fact_listens"
    daily = "gold/" + mart.DAILY_ARTIST
    built_at = datetime.now(timezone.utc)

    def silver_stage():
        clean, rejected = build.build(spark, path("bronze/raw_listens"), built_at=built_at)
        build.write(clean, path(silver), partition=schema.PARTITION_COLUMN)
        build.enable_change_feed(spark, path(silver))
        build.write(rejected, path(quarantine), partition=schema.QUARANTINE_PARTITION_COLUMN)

    stage("silver", [silver, quarantine], silver_stage)
    stage("counts", [counts], lambda: user_recording_counts.refresh(spark, path(silver), path(counts), rebuild=True))

    if silver_only:
        state["production_after"] = identity(production)
        if state["production_after"] != state["production_before"]:
            raise RuntimeError("Production identity or log changed")
        state["completed"] = True
        state["active_stage"] = None
        persist()
        print("silver rebuild completed; Gold not executed", flush=True)
        return state

    def fact_stage():
        prepared = fact_listens.prepare(load(silver), built_at=built_at)
        missing = fact_listens.missing_artists(prepared, load(artist))
        if not missing.isEmpty():
            fact_listens.placeholders(missing, load(artist)).write.format("delta").mode("append").save(path(artist))
        fact_listens.write(prepared, path(facts), silver_version=fact_listens.table_version(spark, path(silver)))

    stage("fact", [facts, artist], fact_stage)

    def dimensions():
        first, last = dim_date.listened_range(load(facts))
        dim_date.write(dim_date.build(spark, first, last), path("gold/" + dates.DATE_TABLE))
        dim_user.write(dim_user.build(load(silver), built_at=built_at), path("gold/" + dates.USER_TABLE))

    stage("dimensions", ["gold/" + dates.DATE_TABLE, "gold/" + dates.USER_TABLE], dimensions)

    def daily_stage():
        result = mart_build.with_names(mart_build.plain(spark, path(facts)), load(artist))
        result.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path(daily))

    stage("daily", [daily], daily_stage)

    def rising_stage():
        (mart_build.rising(load(daily), load("gold/" + dates.DATE_TABLE), built_at=built_at)
         .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
         .save(path("gold/" + mart.RISING_ARTIST)))

    stage("rising", ["gold/" + mart.RISING_ARTIST], rising_stage)
    stage("periods", ["gold/" + mart.PERIOD_ARTIST],
          lambda: artist_periods_build.refresh(spark, path(facts), path("gold/" + mart.PERIOD_ARTIST)))
    state["production_after"] = identity(production)
    if state["production_after"] != state["production_before"]:
        raise RuntimeError("Production identity or log changed")
    state["completed"] = True
    state["active_stage"] = None
    persist()
    print("rebuild completed; independent verification remains", flush=True)
    return state


def main():
    from flux.common.spark import session

    parser = argparse.ArgumentParser()
    parser.add_argument("--production-root", required=True)
    parser.add_argument("--work-root", required=True)
    parser.add_argument("--export-root", required=True)
    parser.add_argument("--memory", default="16g")
    parser.add_argument("--master", default="local[8]")
    parser.add_argument("--silver-only", action="store_true")
    args = parser.parse_args()
    spark = session("flux-rebuild-30-seconds", memory=args.memory, master_url=args.master,
                    shuffle_partitions=256)
    try:
        run(spark, args.production_root, args.work_root, args.export_root, silver_only=args.silver_only)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
