import argparse
import json
from pathlib import Path

from flux.validation.gold_cdf_consume_drill import consume


parser = argparse.ArgumentParser()
parser.add_argument("--exports", type=Path, required=True)
parser.add_argument("--reference", type=Path, required=True)
parser.add_argument("--root", type=Path, required=True)
parser.add_argument("--project", type=Path, default=Path("dbt/flux_gold"))
args = parser.parse_args()
result = consume(args.exports, args.reference, args.root, args.project)
print(json.dumps({key: result[key] for key in ["completed", "guards", "reference_unchanged", "seconds"]}), flush=True)
