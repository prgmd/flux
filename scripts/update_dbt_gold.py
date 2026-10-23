import argparse
import json
from pathlib import Path

from flux.validation.dbt_increment import update


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--previous',type=Path,required=True)
    parser.add_argument('--next-snapshot',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--project',type=Path,default=Path('dbt/flux_gold'))
    parser.add_argument('--policy')
    parser.add_argument('--full-rebuild',action='store_true')
    parser.add_argument('--crash-after-build',action='store_true')
    args=parser.parse_args();data=json.loads(args.next_snapshot.read_text(encoding='utf-8'))
    result=update(args.previous,args.output,data,args.project,policy=args.policy,allow_full_rebuild=args.full_rebuild,
                  crash_after_build=args.crash_after_build)
    print(json.dumps({key:result.get(key) for key in ['completed','impact','checks','failure','production_applied']},ensure_ascii=False))
    return 0 if result['completed'] else 1


if __name__=='__main__':raise SystemExit(main())
