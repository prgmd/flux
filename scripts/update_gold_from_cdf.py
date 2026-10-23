import argparse
import json
from pathlib import Path

from flux.validation.gold_cdf import run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--export", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--project", type=Path, default=Path("dbt/flux_gold"))
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--previous", type=Path)
    source.add_argument("--reference", type=Path)
    parser.add_argument("--crash", choices=["after-build", "before-cursor"])
    args = parser.parse_args()
    result = run(args.export, args.output, args.project, previous=args.previous, reference=args.reference, crash=args.crash)
    print(json.dumps({key: result.get(key) for key in ["completed", "no_changes", "cdf_actions", "checks", "failure"]}, ensure_ascii=False))
    return 0 if result["completed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
