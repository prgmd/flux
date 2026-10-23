import argparse
import json
from pathlib import Path

from flux.validation.gx_pilot import run


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--candidate-db',type=Path)
    args=parser.parse_args()
    result=run(args.baseline,args.output,candidate_db=args.candidate_db)
    print(json.dumps({key:result[key] for key in ['completed','monthly_rows','baseline_unchanged','production_applied','seconds']},ensure_ascii=False))
    return 0 if result['completed'] else 1


if __name__=='__main__':raise SystemExit(main())
