import json
from pathlib import Path

from flux.validation.dbt_pilot import save
from flux.validation.pinning import pin_delta, sha256

TABLE = "monthly_user_recordings"
NAMES = (TABLE, "recording_metadata", "invalid_listens")
COLUMNS = ("user_name", "recording_msid", "listened_month", "play_count")
KIND = "bounded-silver-cdf-export-v1"


def bounded(frame, limit):
    found = frame.limit(limit + 1).collect()
    if len(found) > limit:
        raise ValueError("bounded CDF experiment required")
    return [item.asDict(recursive=True) for item in found]


def cursor_for(root, versions):
    pins = {name: pin_delta(root / name, versions[name]) for name in NAMES}
    states = [pins[name]["monthly_state"] for name in NAMES]
    if not states[0] or any(state != states[0] for state in states):
        raise ValueError("inconsistent Silver checkpoints")
    return {
        "root": str(root), "state": states[0],
        "tables": {name: {field: pins[name][field] for field in ["id", "version", "log_sha256"]} for name in NAMES},
    }


def export(spark, silver, output, *, previous=None):
    from flux.silver.monthly_listens import pin_snapshot

    silver, output = Path(silver).resolve(), Path(output).resolve()
    if silver == output or silver in output.parents or output in silver.parents:
        raise ValueError("separate CDF export required")
    versions = pin_snapshot(spark, silver)
    cursor = cursor_for(silver, versions)
    if previous is not None:
        if previous["root"] != cursor["root"] or previous["state"]["source_id"] != cursor["state"]["source_id"]:
            raise ValueError("Silver source changed")
        old_versions = {name: previous["tables"][name]["version"] for name in NAMES}
        if any(type(value) is not int or value < 0 or value > versions[name] for name, value in old_versions.items()):
            raise ValueError("invalid or regressed source versions")
        if cursor_for(silver, old_versions) != previous:
            raise ValueError("previous Silver history changed")
    end = versions[TABLE]
    start = previous["tables"][TABLE]["version"] if previous is not None else None
    output.mkdir(parents=True, exist_ok=False)
    report = dict(completed=False, kind=KIND, production_applied=False, reader="pyspark-delta",
                  previous_cursor=previous, source_cursor=cursor)
    save(output, report)
    try:
        read = lambda name, version: spark.read.format("delta").option("versionAsOf", version).load(str(silver / name))
        after = bounded(read(TABLE, end).select(*COLUMNS), 10000)
        before = bounded(read(TABLE, start).select(*COLUMNS), 10000) if start is not None else None
        if start is not None and start < end:
            feed = (spark.read.format("delta").option("readChangeFeed", "true")
                    .option("startingVersion", start + 1).option("endingVersion", end)
                    .load(str(silver / TABLE)).select(*COLUMNS, "_change_type", "_commit_version"))
            changes = bounded(feed, 100000)
        else:
            changes = []
        metadata = bounded(read("recording_metadata", versions["recording_metadata"]), 100000)
        payload = dict(before=before, after=after, cdf=changes, metadata=metadata)
        (output / "payload.json").write_text(json.dumps(payload, ensure_ascii=False, allow_nan=False) + "\n", encoding="utf-8")
        if cursor_for(silver, versions) != cursor:
            raise ValueError("pinned Silver changed during export")
        report.update(completed=True, range_read=True, artifacts={"payload.json": sha256(output / "payload.json")},
                      monthly_rows=len(after), cdf_rows=len(changes), starting_version=start, ending_version=end)
    except Exception as error:
        report["failure"] = type(error).__name__ + ": " + str(error)
    save(output, report)
    return report
