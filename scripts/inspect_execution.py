import argparse
import json
from pathlib import Path

from flux.common.execution import inspect_run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('journal', type=Path)
    parser.add_argument('--events', action='store_true')
    args = parser.parse_args()
    result = inspect_run(args.journal)
    if not args.events:
        result.pop('events')
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
