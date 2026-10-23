import argparse
import json
from pathlib import Path

from flux.validation.full_gold_cdf import prepare_source, run_full_cdf


parser=argparse.ArgumentParser()
parser.add_argument('phase',choices=['prepare','consume'])
parser.add_argument('--base',type=Path)
parser.add_argument('--baseline',type=Path,default=Path('data/governance-baseline-20261022-r2'))
parser.add_argument('--source',type=Path)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--project',type=Path,default=Path('dbt/flux_gold'))
args=parser.parse_args()
if args.phase=='prepare':
    if not args.base:parser.error('--base required')
    result=prepare_source(args.base,args.baseline,args.output)
else:
    if not args.source:parser.error('--source required')
    result=run_full_cdf(args.source,args.output,args.project)
print(json.dumps({key:result.get(key) for key in ['completed','stage','failure','elapsed_seconds']},ensure_ascii=False),flush=True)
raise SystemExit(0 if result['completed'] else 1)
