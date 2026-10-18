import argparse
import hashlib
import json
from datetime import datetime, timezone

from flux.common import config
from flux.common.drill_checkpoint import atomic_save, recovered_merge


def main(argv=None, spark=None):
    import fcntl
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from flux.bronze import ingest, merge as bronze_merge, schema as bronze_schema, verify
    from flux.common.spark import session
    from flux.gold.mart_build import fingerprint
    from flux.silver import build, quality, schema, user_recording_counts as counts

    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", required=True)
    parser.add_argument("--work", default="new-day-20261001")
    parser.add_argument("--next-dump")
    parser.add_argument("--next-work")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--interrupt-after-merge", action="store_true")
    args = parser.parse_args(argv)
    if bool(args.next_dump) != bool(args.next_work):
        parser.error("--next-dump와 --next-work를 함께 지정한다")
    root = config.local_root() / config.SCRATCH / args.work
    base = config.scratch(args.work)
    root.resolve().relative_to((config.local_root() / config.SCRATCH).resolve())
    root.mkdir(parents=True, exist_ok=True)
    lock = (root / "verification.lock").open("a+")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    report_path = root / "result.json"
    report = json.loads(report_path.read_text(encoding="utf-8")) if report_path.exists() else {}
    if report and not args.resume:
        raise RuntimeError("기존 실험에는 --resume이 필요하다")
    if report and (report["dump"] != args.dump or report["work"] != args.work):
        raise RuntimeError("실험 입력이 다르다")
    owned = spark is None
    spark = spark or session("flux-new-dump-increment", memory="14g", master_url="local[6]",
                              shuffle_partitions=128,
                              extra={"spark.databricks.delta.snapshotPartitions": "16"})
    paths = {name: base + "/" + name for name in
             ("bronze", "bronze_invalid", "silver", "invalid", "counts")}
    production = {"silver": config.silver(schema.TABLE_NAME),
                  "bronze": config.bronze(bronze_schema.TABLE_NAME)}

    def save():
        report["updated_at"] = datetime.now(timezone.utc).isoformat()
        atomic_save(report_path, report)

    def version(path):
        return DeltaTable.forPath(spark, path).history(1).first()["version"]

    def identity(path):
        return {"id": DeltaTable.forPath(spark, path).detail().first()["id"],
                "version": version(path)}

    def snapshot(path):
        state = identity(path)
        frame = spark.read.format("delta").option("versionAsOf", state["version"]).load(path)
        files = sorted(frame.inputFiles())
        state.update(path=path, files=files,
                     file_digest=hashlib.sha256("\n".join(files).encode()).hexdigest(),
                     rows=frame.count())
        if "play_count" in frame.columns:
            state["plays"] = frame.agg(F.sum("play_count")).first()[0]
        return state

    inputs = [("first", args.dump, args.work)]
    if args.next_dump:
        config.scratch(args.next_work)
        inputs.append(("second", args.next_dump, args.next_work))
    try:
        if not report:
            before = {key: version(path) for key, path in production.items()}
            report.update(dump=args.dump, work=args.work, production_versions_before=before,
                          baseline_quality="production before retrospective playback filter")
            save()
            DeltaTable.forPath(spark, production["silver"]).cloneAtVersion(
                before["silver"], paths["silver"], isShallow=True)
            build.enable_change_feed(spark, paths["silver"])
            report["clone"] = {"version": version(paths["silver"])}
            counts.refresh(spark, paths["silver"], paths["counts"])
            report["initial_silver"] = {"rows": snapshot(paths["silver"])["rows"]}
            save()
        if "runs" not in report:
            if version(paths["silver"]) != report["clone"]["version"]:
                raise RuntimeError("이전 실행의 Silver 상태를 먼저 감사해야 한다")
            if counts.processed_version(spark, paths["counts"]) != version(paths["silver"]):
                raise RuntimeError("준비된 집계의 처리 위치가 다르다")
            report["production_identity_before"] = {k: identity(p) for k, p in production.items()}
            report.update(runs={}, verification_schema=2)
            save()
        plan = [{"slot": slot, "dump": dump, "work": work} for slot, dump, work in inputs]
        if "input_plan" in report and report["input_plan"] != plan:
            raise RuntimeError("이전에 시작한 검증 입력 순서가 다르다")
        report["input_plan"] = plan
        if report.get("playback_threshold_ms", quality.MIN_MS_PLAYED) != quality.MIN_MS_PLAYED:
            raise RuntimeError("실험 도중 품질 기준이 변경됐다")
        report["playback_threshold_ms"] = quality.MIN_MS_PLAYED
        report["same_on_replay"] = False
        save()
        for slot, dump, work in inputs:
            files = sorted((config.local_root() / config.SCRATCH / work / "staging").glob("*.listens"))
            if not files:
                raise RuntimeError("스테이징 파일이 없다")
            for attempt in ("first", "replay"):
                name = slot + "." + attempt
                run = report["runs"].get(name)
                if run and "snapshot" in run:
                    continue
                if run is None:
                    completed = [value for value in report["runs"].values() if "snapshot" in value]
                    expected = completed[-1]["snapshot"]["silver"]["version"] if completed else report["clone"]["version"]
                    if version(paths["silver"]) != expected:
                        raise RuntimeError("단계 사이에 예상하지 않은 Silver 변경이 있다")
                    before_rows = completed[-1]["snapshot"]["silver"]["rows"] if completed else report["initial_silver"]["rows"]
                    run = {"dump": dump, "before_silver": version(paths["silver"]),
                           "before_rows": before_rows,
                           "ingested_at": datetime.now(timezone.utc).isoformat()}
                    report["runs"][name] = run
                    save()
                tag = json.dumps({"drill": args.work, "stage": name}, sort_keys=True)
                if "bronze" not in run:
                    source_lines = sum(verify.count_lines(path) for path in files)
                    if source_lines == 0:
                        raise RuntimeError("빈 덤프는 후속 증분 검증 입력으로 사용하지 않는다")
                    frames = [ingest.build_dataframe(spark, str(path), source_dump=dump,
                              source_file=path.name, ingested_at=datetime.fromisoformat(run["ingested_at"]))
                              for path in files]
                    valid, bad = ingest.split(bronze_merge.combine(frames))
                    ingest.write(valid, paths["bronze"], source_dump=dump)
                    ingest.write(bad, paths["bronze_invalid"], source_dump=dump)
                    raw = spark.read.format("delta").load(paths["bronze"]).filter(F.col("_source_dump") == dump)
                    value = raw.agg(F.count("*").alias("rows"),
                                    fingerprint("raw_json", "_record_id").alias("fingerprint")).first()
                    bad_rows = spark.read.format("delta").load(paths["bronze_invalid"]).filter(
                        F.col("_source_dump") == dump).count()
                    assert source_lines == value.rows + bad_rows
                    run["bronze"] = {"rows": value.rows, "fingerprint": str(value.fingerprint),
                                     "invalid": bad_rows, "source_lines": source_lines}
                    save()
                if "merge" not in run:
                    history = [row.asDict() for row in DeltaTable.forPath(spark, paths["silver"]).history().collect()]
                    merged = recovered_merge(history, run["before_silver"], tag)
                    if merged is None:
                        changes, _ = build.increment(spark, paths["bronze"], paths["silver"],
                                                     dump=dump, built_at=datetime.now(timezone.utc))
                        with counts._commit_metadata(spark, tag):
                            build.merge(spark, changes, paths["silver"])
                        changes.unpersist()
                        if args.interrupt_after_merge:
                            raise RuntimeError("주입한 중단: 병합 커밋 후 결과 저장 전")
                        history = [row.asDict() for row in DeltaTable.forPath(spark, paths["silver"]).history().collect()]
                        merged = recovered_merge(history, run["before_silver"], tag)
                    if merged is None:
                        merged = {"version": version(paths["silver"]), "metrics": {}}
                    run["merge"] = merged
                    save()
                assert version(paths["silver"]) == run["merge"]["version"]
                if "quarantine" not in run:
                    raw = spark.read.format("delta").load(paths["bronze"]).filter(F.col("_source_dump") == dump)
                    rejected = build.with_quality(raw).filter(F.col("_reason").isNotNull())
                    bad = build.quarantine(rejected, built_at=datetime.fromisoformat(run["ingested_at"]))
                    build.replace_quarantine(bad, paths["invalid"], dump=dump)
                    run["quarantine"] = {row.reason: row["count"] for row in
                        bad.groupBy("reason").count().collect()}
                    save()
                counts.refresh(spark, paths["silver"], paths["counts"])
                current = {key: snapshot(paths[key]) for key in ("silver", "counts")}
                watermark = counts._state(spark, paths["counts"])
                assert watermark["version"] == current["silver"]["version"]
                assert watermark["source_id"] == current["silver"]["id"]
                assert current["counts"]["plays"] == current["silver"]["rows"]
                assert current["silver"]["rows"] == run["before_rows"] + run["merge"]["metrics"].get("numTargetRowsInserted", 0)
                if attempt == "replay":
                    first = report["runs"][slot + ".first"]
                    assert run["bronze"] == first["bronze"]
                    assert run["merge"]["metrics"].get("numTargetRowsInserted", 0) == 0
                    assert run["merge"]["metrics"].get("numTargetRowsUpdated", 0) == 0
                    for key in ("silver", "counts"):
                        assert current[key]["rows"] == first["snapshot"][key]["rows"]
                    assert current["counts"]["plays"] == first["snapshot"]["counts"]["plays"]
                run.update(snapshot=current, counts_watermark=watermark)
                save()
                print(name, json.dumps({"silver_version": current["silver"]["version"],
                      "rows": current["silver"]["rows"], "metrics": run["merge"]["metrics"]}), flush=True)
        final_snapshot = list(report["runs"].values())[-1]["snapshot"]
        for key in ("silver", "counts"):
            assert identity(paths[key]) == {name: final_snapshot[key][name] for name in ("id", "version")}
        after = {key: identity(path) for key, path in production.items()}
        assert after == report["production_identity_before"]
        assert {k: v["version"] for k, v in after.items()} == report["production_versions_before"]
        report.update(production_identity_after=after, same_on_replay=True)
        report.pop("failure", None)
        save()
        return 0
    except Exception as exc:
        report["failure"] = {"type": type(exc).__name__, "message": str(exc)}
        save()
        raise
    finally:
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close()
        if owned:
            spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
