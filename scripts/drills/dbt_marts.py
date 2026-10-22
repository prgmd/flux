import argparse
import json

from flux.validation.dbt_drill import drill


parser=argparse.ArgumentParser()
parser.add_argument('--root',required=True)
args=parser.parse_args()
result=drill(args.root)
print(json.dumps({k:result[k] for k in ['drill_passed','scenarios_checked','canonical_database_unchanged']}))
raise SystemExit(0 if result['drill_passed'] else 1)
