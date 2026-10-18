import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote


def copy_verified(source, destination, workers=8):
    source, destination = Path(source), Path(destination)
    logs = sorted(path for path in (source / "_delta_log").glob("*.json")
                  if path.stem.isdigit() and len(path.stem) == 20)
    if [path.stem for path in logs] != ["00000000000000000000"]:
        raise RuntimeError("버전 0인 불변 원본만 캐시한다")
    raw_log = logs[0].read_bytes()
    actions = [json.loads(line) for line in raw_log.splitlines()]
    files = [value["add"] for value in actions if "add" in value]
    metadata = next(value["metaData"] for value in actions if "metaData" in value)
    destination.mkdir(parents=True, exist_ok=False)

    def copy(entry):
        relative = unquote(entry["path"])
        original, target = source / relative, destination / relative
        original.resolve().relative_to(source.resolve())
        target.resolve().relative_to(destination.resolve())
        target.parent.mkdir(parents=True, exist_ok=True)
        digest = hashlib.sha256()
        with original.open("rb") as incoming, target.open("wb") as outgoing:
            while chunk := incoming.read(1024 * 1024):
                digest.update(chunk)
                outgoing.write(chunk)
        with target.open("rb") as copied:
            actual = hashlib.file_digest(copied, "sha256").hexdigest()
        if target.stat().st_size != entry["size"] or digest.hexdigest() != actual:
            raise RuntimeError("캐시 복사 검증 실패: " + relative)
        return relative, actual

    hashes = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for result in pool.map(copy, files):
            hashes.append(result)
            if len(hashes) % 2000 == 0:
                print("verified files", len(hashes), "/", len(files), flush=True)
    if logs[0].read_bytes() != raw_log:
        raise RuntimeError("복사 중 원본 로그가 변경됐다")
    (destination / "_delta_log").mkdir()
    (destination / "_delta_log" / logs[0].name).write_bytes(raw_log)
    certificate = {"source_id": metadata["id"], "version": 0, "files": len(files),
                   "bytes": sum(value["size"] for value in files),
                   "file_hash_digest": hashlib.sha256(json.dumps(sorted(hashes)).encode()).hexdigest(),
                   "log_sha256": hashlib.sha256(raw_log).hexdigest()}
    (destination.parent / "certificate.json").write_text(json.dumps(certificate), encoding="utf-8")
    return certificate


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--destination", required=True)
    args = parser.parse_args()
    print(json.dumps(copy_verified(args.source, args.destination)), flush=True)
