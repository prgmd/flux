import json
import re
from contextlib import contextmanager
from pathlib import Path
from tempfile import NamedTemporaryFile

from flux.validation.pinning import sha256
from flux.validation.quality_gate import verify_artifacts


MARTS = (
    "song_metrics", "artist_metrics", "artist_song_metrics", "coverage",
    "variant_metrics", "listener_metrics", "identity_coverage",
)
ASSETS = {
    "bronze": ("delta_scan", "bronze/raw_listens"),
    "bronze_invalid": ("delta_scan", "bronze/invalid_listens"),
    "monthly_user_recordings": ("delta_scan", "silver/monthly_user_recordings"),
    "recording_metadata": ("delta_scan", "silver/recording_metadata"),
    "monthly_invalid": ("delta_scan", "silver/invalid_listens"),
    **{f"{label}_{mart}": ("read_parquet", f"gold/{period}/{mart}.parquet")
       for label, period in (("monthly", "month"), ("yearly", "year")) for mart in MARTS},
}
POINTER = "ops/current_release.json"


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


def release_dir(root, name):
    if not isinstance(name, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}(?:-[a-z0-9][a-z0-9-]*)?", name):
        raise ValueError("invalid release name")
    base = Path(root).resolve()
    release = (base / "releases" / name).resolve()
    if release != base / "releases" / name:
        raise ValueError("release path resolves outside its declared location")
    return release


def pointer(root):
    path = Path(root) / POINTER
    try:
        return read_json(path)
    except FileNotFoundError:
        return {"active": None, "previous": None}


def resolve(root, entry):
    if entry is None:
        return None
    folder = release_dir(root, entry["release"])
    if sha256(folder / "release.json") != entry["manifest_sha256"]:
        raise ValueError("release manifest changed")
    return folder


def active(root):
    return resolve(root, pointer(root)["active"])


def verify_release(folder):
    manifest = read_json(folder / "release.json")
    if manifest["release"] != folder.name:
        raise ValueError("release manifest name differs")
    files = manifest["files"]
    required = {"evidence/refresh.json", "evidence/silver.json", "evidence/gold.json"}
    for kind, path in ASSETS.values():
        if kind == "delta_scan":
            version = manifest["tables"][path]["version"]
            required.add(f"{path}/_delta_log/{version:020d}.json")
        else:
            required.add(path)
    if not required.issubset(files):
        raise ValueError("release assets incomplete")
    verify_artifacts(folder, files)
    refresh = read_json(folder / "evidence/refresh.json")
    silver = read_json(folder / "evidence/silver.json")
    gold = read_json(folder / "evidence/gold.json")
    if refresh["completed"] is not True or refresh["gold_matches"] is not True:
        raise ValueError("refresh verification incomplete")
    if any(silver[key] != 0 for key in ("duplicate_monthly_keys", "missing_metadata", "invalid_monthly_rows")):
        raise ValueError("Silver quality checks failed")
    recompute = silver["independent_recompute"]
    if recompute["mismatches"] != 0 or recompute["duplicate_keys"] != 0:
        raise ValueError("Silver did not match independent recomputation")
    mart_names = {name for name, (kind, _) in ASSETS.items() if kind == "read_parquet"}
    comparisons = gold["comparisons"]
    if (gold["completed"] is not True or set(comparisons) != mart_names
            or any(value["matches"] is not True for value in comparisons.values())
            or {name: value["rows"] for name, value in comparisons.items()} != manifest["gold_rows"]
            or refresh["gold_rows"] != manifest["gold_rows"]):
        raise ValueError("Gold verification incomplete")


@contextmanager
def locked_pointer(root):
    path = Path(root) / POINTER
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(".lock")
    with lock_path.open("x", encoding="utf-8"):
        pass
    try:
        yield path
    finally:
        lock_path.unlink()


def replace_pointer(path, state):
    with NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent, delete=False) as stream:
        temp = Path(stream.name)
    try:
        temp.write_text(json.dumps(state, indent=2), encoding="utf-8")
        temp.replace(path)
    finally:
        temp.unlink(missing_ok=True)


def publish(root, name):
    folder = release_dir(root, name)
    with locked_pointer(root) as path:
        state = pointer(root)
        current = resolve(root, state["active"])
        verify_release(folder)
        if current == folder:
            return current
        entry = {"release": name, "manifest_sha256": sha256(folder / "release.json")}
        replace_pointer(path, {"active": entry, "previous": state["active"]})
    return folder


def rollback(root, expected):
    with locked_pointer(root) as path:
        state = pointer(root)
        current = resolve(root, state["active"])
        if current is None or current.name != expected:
            raise ValueError("active release does not match rollback request")
        previous = resolve(root, state["previous"])
        if previous is not None:
            verify_release(previous)
        replace_pointer(path, {"active": state["previous"], "previous": state["active"]})
    return previous
