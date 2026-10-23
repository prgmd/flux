import argparse
import json
from pathlib import Path

from flux.validation.dbt_full import run_full

parser=argparse.ArgumentParser()
parser.add_argument('--baseline',required=True,type=Path)
parser.add_argument('--output',required=True,type=Path)
parser.add_argument('--reuse-prepared',type=Path)
parser.add_argument('--resume-failed',action='store_true')
parser.add_argument('--memory-limit',choices=['8GB','16GB'],default='8GB')
args=parser.parse_args()
result=run_full(args.baseline,args.output,Path(__file__).resolve().parents[1]/'dbt/flux_gold',args.reuse_prepared,args.resume_failed,args.memory_limit)
print(json.dumps({k:result.get(k) for k in ['completed','stage','elapsed_seconds','storage','failure']},ensure_ascii=False))
raise SystemExit(0 if result['completed'] else 1)
