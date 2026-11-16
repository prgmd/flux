import argparse
from pathlib import Path

from flux.common.active_release import active, publish, rollback


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("action", choices=("publish", "rollback", "status"))
    parser.add_argument("release", nargs="?")
    args = parser.parse_args()
    if (args.action == "status") == bool(args.release):
        parser.error("publish and rollback require a release name, status takes none")
    if args.action == "publish":
        result = publish(args.root, args.release)
    elif args.action == "rollback":
        result = rollback(args.root, args.release)
    else:
        result = active(args.root)
    print(result if result is not None else "legacy paths")


if __name__ == "__main__":
    main()
