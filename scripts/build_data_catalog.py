import argparse
import json
from pathlib import Path

from flux.validation.data_catalog import build


parser = argparse.ArgumentParser()
parser.add_argument('--dbt-root', required=True, type=Path)
parser.add_argument('--quality-root', required=True, type=Path)
parser.add_argument('--gx-root', required=True, type=Path)
parser.add_argument('--journal', required=True, type=Path)
parser.add_argument('--output', required=True, type=Path)
args = parser.parse_args()
result = build(args.dbt_root, args.quality_root, args.gx_root, args.journal, args.output)
print(json.dumps(result, ensure_ascii=False))
