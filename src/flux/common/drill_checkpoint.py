import json
from pathlib import Path


def atomic_save(path, value):
    path = Path(path)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def recovered_merge(history, before, tag):
    commits = [row for row in history if row["version"] > before]
    if not commits:
        return None
    if len(commits) != 1:
        raise RuntimeError("예상하지 않은 Silver 커밋이 있다")
    row = commits[0]
    if (row["version"] != before + 1 or row.get("userMetadata") != tag
            or row.get("operation") != "MERGE"):
        raise RuntimeError("이번 단계의 병합 커밋이 아니다")
    return {"version": row["version"], "metrics": {
        key: int(value) for key, value in (row.get("operationMetrics") or {}).items()}}
