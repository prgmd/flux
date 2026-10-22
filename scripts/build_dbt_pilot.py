import argparse
import json
from pathlib import Path

from flux.validation.dbt_pilot import bootstrap,execute


parser=argparse.ArgumentParser()
parser.add_argument('--baseline',required=True)
parser.add_argument('--output',required=True,type=Path)
args=parser.parse_args()
bootstrap(args.baseline,args.output)
result=execute(args.output,Path(__file__).resolve().parents[1]/'dbt/flux_gold')
print(json.dumps({k:result.get(k) for k in ['completed','scope','final_comparison','documentation','failure']},ensure_ascii=False))
raise SystemExit(0 if result['completed'] else 1)
