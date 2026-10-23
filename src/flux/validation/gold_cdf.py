import json
import os
import re
import shutil
from collections import Counter, defaultdict
from pathlib import Path

from flux.gold.monthly_marts import VARIANTS, build_period, literal
from flux.validation.dbt_increment import load_sources, previous_proof, project_hashes, update
from flux.validation.dbt_pilot import bootstrap, execute, save
from flux.validation.gold_changes import cdf_periods, index
from flux.validation.oracle import KEYS
from flux.validation.pinning import sha256, verify_bundle
from flux.validation.silver_cdf_export import KIND as EXPORT_KIND, NAMES, TABLE
from flux.silver.quality import MIN_MS_PLAYED

KIND = "bounded-silver-cdf-gold-v1"
FIELDS = ("user_name", "recording_msid", "listened_month", "play_count")


def rule_hashes():
    from flux.gold import monthly_marts
    from flux.silver import quality
    from flux.validation import dbt_increment, gold_changes

    modules = [monthly_marts, quality, dbt_increment, gold_changes]
    paths = [Path(module.__file__) for module in modules] + [Path(__file__)]
    return {path.name: sha256(path) for path in paths}


def monthly_index(items):
    if len(items) > 10000 or any(set(item) != set(FIELDS) for item in items):
        raise ValueError("bounded monthly schema required")
    return index({"monthly": items}, "monthly")


def apply_feed(before, feed, *, start, end):
    if len(feed) > 100000 or any(set(item) != set(FIELDS) | {"_change_type", "_commit_version"} for item in feed):
        raise ValueError("bounded CDF schema required")
    cdf_periods(feed, source_id="verified", previous_source_id="verified", previous_version=start,
                ending_version=end, range_complete=True)
    current = monthly_index(before)
    commits = defaultdict(list)
    for item in feed:
        row = {field: item[field] for field in FIELDS}
        key = next(iter(monthly_index([row])))
        commits[item["_commit_version"]].append((item["_change_type"], key, row))
    for version in sorted(commits):
        removed, added = set(), set()
        for kind, key, row in commits[version]:
            if kind in {"delete", "update_preimage"}:
                if key in removed or current.get(key) != row:
                    raise ValueError("CDF before image does not match previous contribution")
                removed.add(key)
        for key in removed:
            del current[key]
        for kind, key, row in commits[version]:
            if kind in {"insert", "update_postimage"}:
                if key in current or key in added:
                    raise ValueError("duplicate CDF after image")
                current[key] = row
                added.add(key)
    return [current[key] for key in sorted(current)]


def reconcile(before, payload, *, start, end):
    if monthly_index(before) != monthly_index(payload["before"]):
        raise ValueError("Gold input does not match pinned starting Silver")
    actual = apply_feed(before, payload["cdf"], start=start, end=end)
    if monthly_index(actual) != monthly_index(payload["after"]):
        raise ValueError("CDF does not reproduce pinned ending Silver")
    return actual


def validate_cursor(cursor):
    if not isinstance(cursor.get("root"), str) or not cursor["root"] or set(cursor["tables"]) != set(NAMES):
        raise ValueError("invalid Silver cursor")
    state = cursor["state"]
    expected = dict(source_id=state["source_id"], version=state["version"], key=list(FIELDS[:3]),
                    minimum_ms_played=MIN_MS_PLAYED, policy_version=1, timezone="UTC")
    if (state != expected or not isinstance(state["source_id"], str) or not state["source_id"]
            or type(state["version"]) is not int or state["version"] < 0):
        raise ValueError("invalid Silver policy or source checkpoint")
    for entry in cursor["tables"].values():
        if (set(entry) != {"id", "version", "log_sha256"} or not isinstance(entry["id"], str) or not entry["id"]
                or type(entry["version"]) is not int or entry["version"] < 0
                or not isinstance(entry["log_sha256"], str) or not re.fullmatch("[0-9a-f]{64}", entry["log_sha256"])):
            raise ValueError("invalid pinned table identity")


def bundle(root):
    root = Path(root).resolve()
    report = verify_bundle(root)
    if (report.get("kind") != EXPORT_KIND or report.get("range_read") is not True
            or report.get("reader") != "pyspark-delta" or report.get("production_applied") is not False):
        raise ValueError("verified actual Silver CDF export required")
    if set(report.get("artifacts", {})) != {"payload.json"}:
        raise ValueError("hashed CDF payload required")
    if (root / "payload.json").stat().st_size > 64 * 1024 * 1024:
        raise ValueError("bounded CDF bundle required")
    payload = json.loads((root / "payload.json").read_text(encoding="utf-8"))
    if (set(payload) != {"before", "after", "cdf", "metadata"}
            or len(payload["cdf"]) > 100000 or len(payload["metadata"]) > 100000):
        raise ValueError("bounded complete CDF payload required")
    monthly_index(payload["after"])
    cursor = report["source_cursor"]
    validate_cursor(cursor)
    if report["ending_version"] != cursor["tables"][TABLE]["version"]:
        raise ValueError("invalid Silver cursor")
    previous = report["previous_cursor"]
    if previous is not None:
        validate_cursor(previous)
        if (previous["root"] != cursor["root"] or previous["state"]["source_id"] != cursor["state"]["source_id"]
                or previous["state"]["version"] > cursor["state"]["version"]
                or report["starting_version"] != previous["tables"][TABLE]["version"]
                or any(previous["tables"][name]["id"] != cursor["tables"][name]["id"]
                       or previous["tables"][name]["version"] > cursor["tables"][name]["version"] for name in NAMES)):
            raise ValueError("CDF range does not follow previous cursor")
    elif report["starting_version"] is not None or payload["before"] is not None or payload["cdf"]:
        raise ValueError("invalid initial CDF snapshot")
    return report, payload


def proof(root):
    root = Path(root).resolve()
    result = verify_bundle(root)
    if result.get("kind") != KIND or result.get("production_applied") is not False:
        raise ValueError("completed isolated CDF Gold snapshot required")
    if json.loads((root / "cursor.json").read_text(encoding="utf-8")) != result["source_cursor"]:
        raise ValueError("cursor changed")
    validate_cursor(result["source_cursor"])
    previous_proof(root / "gold")
    return result


def extend_context(data, monthly, metadata):
    result = json.loads(json.dumps(data))
    result["monthly"] = monthly
    known = {row["recording_msid"] for row in data["dimension"]}
    grouped = defaultdict(list)
    for row in metadata:
        grouped[row["recording_msid"]].append(row)
    for msid in sorted({row["recording_msid"] for row in monthly} - known):
        values = grouped.get(msid)
        if not values:
            raise ValueError("new MSID is missing pinned metadata")
        title = min(row.get("track_name") or "[unknown]" for row in values)
        artist = min(row.get("artist_name") or "[unknown]" for row in values)
        variants = set()
        for row in values:
            labels = " ".join(re.findall(r"\([^()]*\)|\[[^\[\]]*\]| - .*$", row.get("track_name") or "")).lower()
            variants.add("+".join(name for name, pattern in VARIANTS.items() if re.search(pattern, labels)) or "unmarked")
        variant = next(iter(variants)) if len(variants) == 1 else "conflicting_labels"
        key = "msid:" + msid
        result["dimension"].append(dict(recording_msid=msid, recording_mbid=None, status="unreviewed",
                                        song_key=key, song_name=title, observed_artist=artist, variant=variant))
        result["labels"].append(dict(song_key=key, song_name=title, observed_artist=artist, variant=variant,
                                     identity_status="unreviewed", source_msids=1))
    return result


def seed_baseline(root, data, state):
    import duckdb

    root.mkdir()
    (root / "inputs").mkdir()
    (root / "source_manifests").mkdir()
    types = {
        "monthly": "user_name varchar,recording_msid varchar,listened_month varchar,play_count bigint",
        "dimension": "recording_msid varchar,recording_mbid varchar,status varchar,song_key varchar,song_name varchar,observed_artist varchar,variant varchar",
        "labels": "song_key varchar,song_name varchar,observed_artist varchar,variant varchar,identity_status varchar,source_msids bigint",
        "credits": "song_key varchar,artist_mbid varchar,artist_name varchar",
    }
    names = dict(monthly="monthly", dimension="song_dimension", labels="song_labels", credits="song_credits")
    with duckdb.connect() as connection:
        for name, table in names.items():
            connection.execute("create table " + table + "(" + types[name] + ")")
            file = root / "inputs" / (table + ".json")
            file.write_text(json.dumps(data[name], ensure_ascii=False) + "\n", encoding="utf-8")
            schema = connection.execute("describe " + table).fetchall()
            columns = "{" + ",".join(literal(row[0]) + ":" + literal(row[1]) for row in schema) + "}"
            connection.execute("insert into " + table + " select * from read_json(?,columns=" + columns + ",format='array')", [str(file)])
            connection.execute("copy " + table + " to ? (format parquet)", [str(root / "inputs" / (table + ".parquet"))])
        for period in ["month", "year"]:
            target = root / "expected" / period
            target.mkdir(parents=True)
            build_period(connection, period)
            for table in KEYS:
                connection.execute("copy " + table + " to ? (format parquet)", [str(target / (table + ".parquet"))])
    context = {"r9_reference_only": True, "gold_policies": {"timezone": "UTC"}, "tables": {
        "bronze": {"id": state["source_id"], "version": state["version"]},
        **{name: {"monthly_state": state} for name in NAMES},
    }}
    for name, value in {"source_context.json": context, "compatibility.json": {"listener_key": "user_name"},
                        "source_manifests/r9_production_export.json": {"approved_candidates": 0, "mappings": []}}.items():
        (root / name).write_text(json.dumps(value), encoding="utf-8")
    save(root, dict(completed=True, artifacts={path.relative_to(root).as_posix(): sha256(path)
               for path in root.rglob("*") if path.is_file()}))


def run(exported, output, project, *, previous=None, reference=None, crash=None):
    exported, output, project = map(lambda value: Path(value).resolve(), [exported, output, project])
    if (previous is None) == (reference is None):
        raise ValueError("exactly one previous CDF snapshot or reference Gold required")
    parent = Path(previous if previous is not None else reference).resolve()
    for source in [exported, parent, project]:
        if source == output or source in output.parents or output in source.parents:
            raise ValueError("separate CDF Gold output required")
    incoming, payload = bundle(exported)
    code = project_hashes(project)
    rules = rule_hashes()
    already_consumed = False
    if previous is not None:
        old = proof(parent)
        if old["project_sha256"] != code or old["rules_sha256"] != rules:
            raise ValueError("CDF checkpoint or Gold project changed")
        data = load_sources(parent / "gold")
        already_consumed = incoming["source_cursor"] == old["source_cursor"]
        if already_consumed:
            if payload["before"] is not None:
                reconcile(payload["before"], payload, start=incoming["starting_version"], end=incoming["ending_version"])
            if monthly_index(data["monthly"]) != monthly_index(payload["after"]):
                raise ValueError("already consumed CDF does not match current Gold input")
            monthly = data["monthly"]
        else:
            if incoming["previous_cursor"] != old["source_cursor"]:
                raise ValueError("CDF checkpoint or Gold project changed")
            monthly = reconcile(data["monthly"], payload, start=incoming["starting_version"], end=incoming["ending_version"])
    else:
        previous_proof(parent)
        if incoming["previous_cursor"] is not None or payload["before"] is not None or payload["cdf"]:
            raise ValueError("initial Silver snapshot required")
        data = load_sources(parent)
        monthly = payload["after"]
    next_data = extend_context(data, monthly, payload["metadata"])
    parent_digest = sha256(parent / "result.json")
    incoming_digest = sha256(exported / "result.json")
    output.mkdir(parents=True, exist_ok=False)
    report = dict(completed=False, kind=KIND, production_applied=False, promotion_ready=False, already_consumed=already_consumed,
                  source_cursor=incoming["source_cursor"], project_sha256=code, rules_sha256=rules,
                  previous={"path": str(parent), "result_sha256": parent_digest},
                  scope="bounded actual Silver CDF; no operating data promotion")
    save(output, report)
    try:
        shutil.copytree(exported, output / "cdf")
        if previous is None:
            seed_baseline(output / "baseline", next_data, incoming["source_cursor"]["state"])
            bootstrap(output / "baseline", output / "gold")
            result = execute(output / "gold", project)
        else:
            result = update(parent / "gold", output / "gold", next_data, project,
                            crash_after_build=crash == "after-build")
        if result["completed"] is not True:
            raise ValueError("Gold build did not complete: " + str(result.get("failure")))
        if crash == "before-cursor":
            os._exit(86)
        (output / "cursor.json").write_text(json.dumps(incoming["source_cursor"], ensure_ascii=False) + "\n", encoding="utf-8")
        if sha256(parent / "result.json") != parent_digest or sha256(exported / "result.json") != incoming_digest:
            raise ValueError("parent or exported manifest changed")
        if rule_hashes() != rules:
            raise ValueError("CDF or metric rules changed during build")
        bundle(exported)
        bundle(output / "cdf")
        if sha256(output / "cdf/result.json") != incoming_digest:
            raise ValueError("copied CDF manifest changed")
        proof(parent) if previous is not None else previous_proof(parent)
        report.update(completed=True, no_changes=result.get("no_changes", False), impact=result.get("impact"),
                      checks=result.get("checks", result.get("final_comparison")),
                      cdf_rows=len(payload["cdf"]), cdf_actions=dict(Counter(row["_change_type"] for row in payload["cdf"])),
                      artifacts={name: sha256(output / name) for name in ["cursor.json", "gold/result.json", "cdf/result.json", "cdf/payload.json"]})
    except Exception as error:
        report["failure"] = type(error).__name__ + ": " + str(error)
    save(output, report)
    return report
