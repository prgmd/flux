import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from flux.validation.cdf_drill import measure
from flux.validation.dbt_pilot import save
from flux.validation.pinning import sha256
from flux.validation.silver_cdf_export import export

NOW = datetime(2026, 10, 23, tzinfo=timezone.utc)


def tree_hashes(root):
    return {path.relative_to(root).as_posix(): sha256(path) for path in root.rglob("*") if path.is_file()}


def produce(spark, source, root):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from flux.silver import build, monthly_listens as monthly

    source, root = Path(source).resolve(), Path(root).resolve()
    if source == root or source in root.parents or root in source.parents:
        raise ValueError("separate experiment required")
    original = json.loads((source / "result.json").read_text(encoding="utf-8"))
    if (original.get("completed") is not True or original.get("production_applied") is not False
            or original.get("scope") != "retained real dump prefixes and pinned Bronze probe; not fresh full dumps"):
        raise ValueError("completed bounded real dump experiment required")
    hashes = {name: tree_hashes(source / name) for name in ["bronze", "silver"]}
    if sum(path.stat().st_size for name in hashes for path in (source / name).rglob("*") if path.is_file()) > 100 * 1024 * 1024:
        raise ValueError("bounded source copy required")
    root.mkdir(parents=True, exist_ok=False)
    report = dict(completed=False, production_applied=False, scenarios=[], source_manifest_sha256=sha256(source / "result.json"))
    save(root, report)
    for name in hashes:
        shutil.copytree(source / name, root / name)
        if tree_hashes(root / name) != hashes[name]:
            raise ValueError("source copy changed")
    bronze, silver = root / "bronze", root / "silver"
    raw = spark.read.format("delta").load(str(bronze))
    if raw.limit(10001).count() > 10000:
        raise ValueError("bounded Bronze probe required")
    counts = spark.read.format("delta").load(str(silver / monthly.TABLE))
    chosen = counts.orderBy("user_name", "recording_msid", "listened_month").first().asDict()
    candidate = (build.with_quality(raw).filter(F.col("_reason").isNull())
                 .filter((F.col("user_name") == chosen["user_name"])
                         & (F.get_json_object("raw_json", "$.recording_msid") == chosen["recording_msid"])
                         & (F.date_format(F.from_unixtime("listened_at"), "yyyy-MM") == chosen["listened_month"])))
    item = candidate.select(*raw.columns).first().asDict()
    occupied = {row.listened_at for row in raw.filter(F.col("user_name") == chosen["user_name"]).select("listened_at").collect()}
    stamp = next(item["listened_at"] + delta for step in range(1, 100) for delta in [step, -step]
                 if item["listened_at"] + delta not in occupied
                 and datetime.fromtimestamp(item["listened_at"] + delta, timezone.utc).strftime("%Y-%m") == chosen["listened_month"])
    years = {row.listened_month[:4] for row in counts.select("listened_month").distinct().collect()}
    new_year = next(year for year in range(2000, 2026) if str(year) not in years)
    moved = int(datetime(new_year, 2, 1, tzinfo=timezone.utc).timestamp())
    table = DeltaTable.forPath(spark, str(bronze))
    cursor = None

    def capture(name):
        nonlocal cursor
        checked = measure(spark, bronze, silver, name)
        result = export(spark, silver, root / "exports" / name, previous=cursor)
        if not result["completed"]:
            raise ValueError(result.get("failure"))
        cursor = result["source_cursor"]
        report["scenarios"].append(dict(name=name, export="exports/" + name, silver=checked,
                                        cursor=cursor, cdf_rows=result["cdf_rows"]))
        save(root, report)

    def append(number, timestamp):
        added = dict(item)
        document = json.loads(item["raw_json"])
        document["timestamp"] = timestamp
        added.update(_source_dump="__gold_cdf_probe", _source_file="controlled real-record copy", _record_id=number,
                     listened_at=timestamp, raw_json=json.dumps(document), _ingested_at=NOW, ingested_date=NOW.date())
        spark.createDataFrame([tuple(added[name] for name in raw.columns)], raw.schema).write.format("delta").mode("append").save(str(bronze))
        monthly.refresh(spark, str(bronze), silver, built_at=NOW)

    capture("initial")
    monthly.refresh(spark, str(bronze), silver, built_at=NOW)
    capture("replay")
    append(1, stamp)
    capture("updated_count")
    document = json.loads(item["raw_json"])
    document["timestamp"] = moved
    table.update("_source_dump='__gold_cdf_probe' AND _record_id=1",
                 {"listened_at": F.lit(moved), "raw_json": F.lit(json.dumps(document))})
    monthly.refresh(spark, str(bronze), silver, built_at=NOW)
    capture("moved_year")
    table.delete("_source_dump='__gold_cdf_probe'")
    monthly.refresh(spark, str(bronze), silver, built_at=NOW)
    capture("deleted_last_year")
    spark.sql(f"ALTER TABLE delta.`{bronze}` SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 31 days')")
    monthly.refresh(spark, str(bronze), silver, built_at=NOW)
    capture("metadata_only")
    append(2, stamp)
    capture("recovery_input")
    if any(tree_hashes(source / name) != hashes[name] for name in hashes):
        raise ValueError("original experiment changed")
    actions = {}
    for entry in report["scenarios"]:
        payload = json.loads((root / entry["export"] / "payload.json").read_text(encoding="utf-8"))
        for row in payload["cdf"]:
            actions[row["_change_type"]] = actions.get(row["_change_type"], 0) + 1
    if not all(actions.get(name, 0) > 0 for name in ["insert", "delete", "update_preimage", "update_postimage"]):
        raise ValueError("actual insert, delete and both update images required")
    report.update(completed=True, original_unchanged=True, cdf_actions=actions, inserted_year=new_year,
                  spark_version=spark.version, scope="bounded retained real records with controlled changes")
    save(root, report)
    return report
