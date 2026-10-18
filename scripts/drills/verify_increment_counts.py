import argparse
import hashlib
import json
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from urllib.parse import unquote, urlsplit

from flux.common.drill_checkpoint import atomic_save


def local_files(snapshot, data_root):
    root = Path(data_root).resolve()
    files = []
    for raw in snapshot["files"]:
        value = unquote(urlsplit(raw).path) if raw.startswith("file:") else raw
        path = root / value[len("/data/"):] if value.startswith("/data/") else Path(value)
        path.resolve().relative_to(root)
        if not path.is_file():
            raise FileNotFoundError(path)
        files.append(str(path))
    return files


def compare(connection, silver_files, counts_files):
    connection.from_parquet(silver_files).create_view("silver")
    connection.from_parquet(counts_files).create_view("actual")
    row = connection.execute("""
        WITH expected AS (
            SELECT user_id, recording_msid, count(*) AS plays
            FROM silver GROUP BY user_id, recording_msid
        )
        SELECT count(*) FILTER (WHERE e.plays IS DISTINCT FROM a.play_count),
               sum(e.plays), sum(a.play_count), count(*)
        FROM expected e FULL OUTER JOIN actual a USING (user_id, recording_msid)
    """).fetchone()
    invalid = connection.execute("""
        SELECT count(*), count(DISTINCT (user_id, recording_msid)),
               count(*) FILTER (WHERE play_count <= 0 OR play_count IS NULL
                                 OR user_id IS NULL OR recording_msid IS NULL)
        FROM actual
    """).fetchone()
    result = dict(zip(("mismatched_pairs", "expected_plays", "actual_plays", "compared_pairs"), row))
    result.update(actual_rows=invalid[0], distinct_pairs=invalid[1], invalid_rows=invalid[2])
    result["ok"] = row[0] == 0 and invalid[0] == invalid[1] and invalid[2] == 0
    return result


def verify_inputs(report, root):
    from flux.silver.quality import judge

    results = {}
    for item in report["input_plan"]:
        run = report["runs"][item["slot"] + ".first"]
        epoch = datetime.fromisoformat(run["ingested_at"]).timestamp()
        staging = root / "scratch" / item["work"] / "staging"
        staging.resolve().relative_to((root / "scratch").resolve())
        lines, invalid = 0, 0
        reasons = Counter()
        for path in sorted(staging.glob("*.listens")):
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    lines += 1
                    try:
                        document = json.loads(line)
                    except ValueError:
                        invalid += 1
                        continue
                    if not isinstance(document, dict):
                        invalid += 1
                        continue
                    verdict = judge(document, ingested_at=epoch)
                    if not verdict.ok:
                        reasons[verdict.reason] += 1
        assert lines == run["bronze"]["source_lines"]
        assert invalid == run["bronze"]["invalid"]
        assert dict(reasons) == run["quarantine"]
        results[item["slot"]] = {"source_lines": lines, "invalid_json": invalid,
                                  "python_quarantine": dict(reasons), "ok": True}
        print(item["slot"] + ": input quality matches", json.dumps(results[item["slot"]]), flush=True)
    return results


def audit_production(report, root):
    tables = {"bronze": "bronze/raw_listens", "silver": "silver/listens_normalized"}
    actual = {}
    for name, relative in tables.items():
        log = root / relative / "_delta_log"
        commits = sorted(path for path in log.glob("*.json")
                         if len(path.stem) == 20 and path.stem.isdigit())
        metadata = None
        for path in commits:
            for line in path.read_text(encoding="utf-8").splitlines():
                value = json.loads(line)
                if "metaData" in value:
                    metadata = value["metaData"]
        actual[name] = {"id": metadata["id"], "version": int(commits[-1].stem)}
    assert actual == report["production_identity_before"]
    return actual


def main(argv=None):
    import duckdb

    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", required=True)
    parser.add_argument("--work", default="new-day-20261001")
    args = parser.parse_args(argv)
    root = Path(args.data_root)
    work = root / "scratch" / args.work
    work.resolve().relative_to((root / "scratch").resolve())
    report = json.loads((work / "result.json").read_text(encoding="utf-8"))
    assert report["same_on_replay"] is True
    connection = duckdb.connect()
    connection.execute("SET threads=4")
    connection.execute("SET memory_limit='12GB'")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("SET temp_directory='" + str(work / "oracle-temp").replace("\\", "/").replace("'", "''") + "'")
    output = {"runs": {}, "baseline_quality": report["baseline_quality"],
              "host_production_identity": audit_production(report, root)}
    output["inputs"] = verify_inputs(report, root)
    expected_path = work / "expected_silver.json"
    if expected_path.exists():
        expected = json.loads(expected_path.read_text(encoding="utf-8"))
        first = report["runs"]["first.first"]
        assert first["bronze"]["rows"] == expected["raw_rows"]
        assert first["merge"]["metrics"].get("numTargetRowsInserted", 0) == expected["expected_inserts"]
        assert first["merge"]["metrics"].get("numTargetRowsUpdated", 0) == expected["expected_updates"]
        output["first_silver_matches_prior_independent_expected"] = True
    cache = {}
    for name, run in report["runs"].items():
        before = time.monotonic()
        silver, counts = run["snapshot"]["silver"], run["snapshot"]["counts"]
        assert run["counts_watermark"]["version"] == silver["version"]
        assert run["counts_watermark"]["source_id"] == silver["id"]
        files = local_files(silver, root), local_files(counts, root)
        digest = hashlib.sha256(json.dumps(files).encode()).hexdigest()
        if digest in cache:
            result = dict(cache[digest], reused_identical_files=True)
        else:
            print(name + ": independent full aggregation started", flush=True)
            result = compare(connection, *files)
            cache[digest] = result
        result = dict(result, silver_version=silver["version"], counts_version=counts["version"],
                      seconds=round(time.monotonic() - before, 1))
        output["runs"][name] = result
        atomic_save(work / "oracle.json", output)
        print(name, json.dumps(result), flush=True)
        assert result["ok"], name
    output["ok"] = all(value["ok"] for value in output["runs"].values())
    atomic_save(work / "oracle.json", output)
    connection.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
