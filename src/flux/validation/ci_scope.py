def select_engines(paths, *, force=False):
    paths = [path.replace("\\", "/") for path in paths]
    shared = force or any(
        path.startswith(("src/", "tests/", "scripts/", ".github/workflows/"))
        or path in {"requirements-dev.txt", "pyproject.toml", "pytest.ini"}
        for path in paths
    )
    return {
        "dbt": bool(shared or any(
            path.startswith("dbt/") or path == "requirements-dbt.txt"
            for path in paths
        )),
        "gx": bool(shared or "requirements-gx.txt" in paths),
    }


def event_base(event, payload):
    if event == "pull_request":
        return payload.get("pull_request", {}).get("base", {}).get("sha")
    value = payload.get("before") if event == "push" else None
    return value if value and set(value) != {"0"} else None
