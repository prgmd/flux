import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from flux.validation.dbt_pilot import save
from flux.validation.gold_cdf import proof, run
from flux.validation.pinning import sha256


def consume(exports, reference, output, project):
    exports, reference, output, project = map(lambda value: Path(value).resolve(), [exports, reference, output, project])
    for source in [exports, reference, project]:
        if source == output or source in output.parents or output in source.parents:
            raise ValueError("separate consumer experiment required")
    produced = json.loads((exports / "result.json").read_text(encoding="utf-8"))
    if produced.get("completed") is not True or produced.get("original_unchanged") is not True:
        raise ValueError("completed actual CDF producer required")
    before = {name: sha256(reference / name) for name in ["pilot.duckdb", "result.json"]}
    output.mkdir(parents=True, exist_ok=False)
    report = dict(completed=False, production_applied=False, scenarios=[], guards={},
                  producer_manifest_sha256=sha256(exports / "result.json"))
    save(output, report)
    previous = None
    base_plays = None
    begin = time.perf_counter()
    for name in ["initial", "replay", "updated_count", "moved_year", "deleted_last_year", "metadata_only"]:
        target = output / name
        result = run(exports / "exports" / name, target, project,
                     **({"previous": previous} if previous is not None else {"reference": reference}))
        if not result["completed"]:
            raise ValueError(result.get("failure"))
        plays = result["checks"]["plays"]
        if name == "initial":
            base_plays = plays
            try:
                run(exports / "exports/moved_year", output / "forbidden-gap", project, previous=target)
            except ValueError:
                report["guards"]["skipped_CDF_cursor"] = not (output / "forbidden-gap").exists()
            else:
                raise ValueError("CDF gap was accepted")
        if plays != base_plays + (1 if name in {"updated_count", "moved_year"} else 0):
            raise ValueError("unexpected CDF total")
        if name in {"replay", "metadata_only"} and result["no_changes"] is not True:
            raise ValueError("replay or metadata-only commit rebuilt marts")
        if name == "replay" and result["source_cursor"] != proof(previous)["source_cursor"]:
            raise ValueError("replay advanced source versions")
        if name == "metadata_only" and result["source_cursor"] == proof(previous)["source_cursor"]:
            raise ValueError("metadata-only version was not recorded")
        report["scenarios"].append(dict(name=name, plays=plays, no_changes=result["no_changes"],
                                        checks=result["checks"], source_cursor=result["source_cursor"],
                                        cdf_actions=result["cdf_actions"], impact=result["impact"]))
        previous = target
        if name == "updated_count":
            repeated = output / "repeated-batch"
            replay = run(exports / "exports" / name, repeated, project, previous=previous)
            if (not replay["completed"] or not replay["no_changes"] or not replay["already_consumed"]
                    or replay["checks"]["plays"] != base_plays + 1):
                raise ValueError("repeated CDF batch was counted again")
            report["scenarios"].append(dict(name="repeated-batch", plays=replay["checks"]["plays"],
                                            no_changes=True, already_consumed=True, checks=replay["checks"],
                                            source_cursor=replay["source_cursor"], cdf_actions=replay["cdf_actions"]))
            previous = repeated
        save(output, report)
    parent_digest = sha256(previous / "result.json")
    parent_cursor = (previous / "cursor.json").read_bytes()
    script = Path(__file__).resolve().parents[3] / "scripts/update_gold_from_cdf.py"
    next_export = exports / "exports/recovery_input"
    for phase in ["after-build", "before-cursor"]:
        crashed = output / ("crash-" + phase)
        command = [sys.executable, str(script), "--export", str(next_export), "--previous", str(previous),
                   "--output", str(crashed), "--project", str(project), "--crash", phase]
        with (output / (phase + ".log")).open("w", encoding="utf-8") as stream:
            process = subprocess.run(command, env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[2])},
                                     stdout=stream, stderr=subprocess.STDOUT, timeout=300)
        if process.returncode != 86 or (crashed / "cursor.json").exists():
            raise ValueError("actual interruption before cursor commit required: " + str(process.returncode))
        try:
            proof(crashed)
        except ValueError:
            report["guards"]["incomplete_" + phase] = True
        else:
            raise ValueError("incomplete CDF snapshot accepted")
        recovered = output / ("recovered-" + phase)
        result = run(next_export, recovered, project, previous=previous)
        if not result["completed"] or result["checks"]["plays"] != base_plays + 1:
            raise ValueError("CDF recovery differs: " + str(result.get("failure")))
        if sha256(previous / "result.json") != parent_digest or (previous / "cursor.json").read_bytes() != parent_cursor:
            raise ValueError("last completed cursor changed")
        report["scenarios"].append(dict(name="recovered-" + phase, plays=result["checks"]["plays"],
                                        checks=result["checks"], source_cursor=result["source_cursor"],
                                        cdf_actions=result["cdf_actions"], forced_exit_code=process.returncode))
        save(output, report)
    tampered = output / "tampered-export"
    shutil.copytree(next_export, tampered)
    with (tampered / "payload.json").open("a", encoding="utf-8") as stream:
        stream.write("\n")
    try:
        run(tampered, output / "forbidden-tamper", project, previous=previous)
    except ValueError:
        report["guards"]["changed_export"] = not (output / "forbidden-tamper").exists()
    else:
        raise ValueError("changed export accepted")
    if not all(report["guards"].values()):
        raise ValueError("CDF guard failed")
    if any(sha256(reference / name) != digest for name, digest in before.items()):
        raise ValueError("reference Gold changed")
    report.update(completed=True, reference_unchanged=True, last_cursor_unchanged=True,
                  seconds=round(time.perf_counter() - begin, 3),
                  scope="bounded real Silver CDF and dbt; not full Silver or new dump operation")
    save(output, report)
    return report
