import argparse
import json
import time
from pathlib import Path

from flux.common.spark import session
from flux.common.drill_checkpoint import atomic_save
from flux.silver import monthly_listens as monthly
from flux.silver.monthly_state import exclusive, separate


def run(spark, verified_root, output):
    from rebuild_playback import table_state

    verified_root, output = Path(verified_root), Path(output)
    separate(verified_root, output)
    checks = json.loads((verified_root / "verification.json").read_text(encoding="utf-8"))
    certificate = json.loads((verified_root / "completion.json").read_text(encoding="utf-8"))
    snapshots = json.loads((verified_root / "verification-snapshots.json").read_text(encoding="utf-8"))
    baseline = json.loads((verified_root / "rebuild.json").read_text(encoding="utf-8"))
    if not checks.get("ok") or not certificate.get("completed_at"):
        raise ValueError("완료된 전수 검증 증거가 필요하다")
    if baseline.get("minimum_ms_played") != monthly.MIN_MS_PLAYED:
        raise ValueError("기존 Silver와 현재 품질 기준이 다르다")
    before = {}
    for name in ("bronze", "silver", "quarantine"):
        relative = {"bronze": "bronze/raw_listens", "silver": "silver/listens_normalized",
                    "quarantine": "silver/invalid_listens"}[name]
        path = verified_root / relative
        current = table_state(path)
        if current != snapshots[name]["state"]:
            raise RuntimeError("검증 이후 원본 표가 바뀌었다: " + name)
        before[name] = {"path": str(path), "state": current}
    state = monthly.state_for(before["bronze"]["state"]["id"], before["bronze"]["state"]["version"])
    manifest_path = output / "migration.json"
    with exclusive(output):
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest["sources"] != before:
                raise RuntimeError("재개할 원본이 달라졌다")
        else:
            manifest = {"sources": before, "stages": {}, "completed": False,
                        "gold_executed": False, "key": list(monthly.KEY)}
            atomic_save(manifest_path, manifest)
        detail = (spark.read.format("delta").option("versionAsOf", before["silver"]["state"]["version"])
                  .load(before["silver"]["path"]))
        frames = {
            monthly.TABLE: lambda: monthly.aggregate(detail).repartition(32, "listened_month")
                .sortWithinPartitions("listened_month", "user_name", "recording_msid"),
            monthly.METADATA: lambda: detail.select(*monthly.VARIANT).distinct()
                .repartition(16, "recording_msid").sortWithinPartitions("recording_msid"),
            monthly.QUARANTINE: lambda: spark.read.format("delta")
                .option("versionAsOf", before["quarantine"]["state"]["version"])
                .load(before["quarantine"]["path"]),
        }
        for name, frame in frames.items():
            path = output / name
            done = monthly.checkpoint(spark, path)
            if done is not None and done != state:
                raise RuntimeError("기존 출력의 원본 또는 처리 정책이 다르다")
            if done == state:
                print("resume:", name, flush=True)
                continue
            print("start:", name, flush=True)
            started = time.monotonic()
            monthly.create(frame(), path, state,
                           partition="listened_month" if name == monthly.TABLE else None)
            manifest["stages"][name] = {"seconds": round(time.monotonic() - started, 2)}
            atomic_save(manifest_path, manifest)
            print("done:", name, manifest["stages"][name], flush=True)
        for record in before.values():
            if table_state(Path(record["path"])) != record["state"]:
                raise RuntimeError("실행 중 원본이 바뀌었다")
        if not monthly.ready(spark, output):
            raise RuntimeError("표별 체크포인트 불일치")
        manifest["completed"] = True
        manifest["source_unchanged"] = True
        manifest["independent_validation_pending"] = True
        atomic_save(manifest_path, manifest)
        return manifest


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--verified-root", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    spark = session("flux-compact-monthly", master_url="local[4]", memory="8g",
                    shuffle_partitions=64)
    try:
        print(json.dumps(run(spark, args.verified_root, args.output)), flush=True)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
