import argparse
import json
from pathlib import Path

from flux.validation.dbt_period_drill import metadata_policy_drill


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--previous',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--project',type=Path,default=Path('dbt/flux_gold'))
    args=parser.parse_args();result=metadata_policy_drill(args.previous,args.output,args.project)
    print(json.dumps({key:result[key] for key in ['completed','scope','production_applied','previous_unchanged']},ensure_ascii=False))
    return 0 if result['completed'] else 1


if __name__=='__main__':raise SystemExit(main())
