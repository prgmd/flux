import argparse
import json
import os
import subprocess
from pathlib import Path

from flux.validation.ci_scope import event_base,select_engines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base")
    args = parser.parse_args()
    event = os.environ.get("GITHUB_EVENT_NAME", "local")
    payload = {}
    if os.environ.get("GITHUB_EVENT_PATH"):
        payload = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text(encoding="utf-8"))
    base = args.base or event_base(event, payload)
    force = os.environ.get("FORCE_INTEGRATIONS", "false").lower() == "true"
    command = ["git", "diff", "--name-only", "-z", base, "HEAD"] if base else ["git", "ls-files", "-z"]
    if event == "workflow_dispatch" and not base:
        paths = []
    else:
        try:
            paths = [path for path in subprocess.check_output(command).decode("utf-8").split("\0") if path]
        except (subprocess.CalledProcessError, UnicodeError):
            paths = []
            force = True
    engines = select_engines(paths, force=force)
    if os.environ.get("GITHUB_OUTPUT"):
        with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as stream:
            for name, value in engines.items():
                stream.write(name + "=" + str(value).lower() + "\n")
    print(json.dumps({"event": event, "changed_files": len(paths), "engines": engines}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
