import argparse
import json
from pathlib import Path

from flux.validation.snapshot_backup import backup, restore, verify_delta


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('phase',choices=['backup','restore','verify-delta'])
    parser.add_argument('--source',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--project',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--approval',type=Path)
    args=parser.parse_args()
    if args.phase=='backup':
        if not args.approval:parser.error('--approval required')
        result=backup(args.source,args.output,args.project,args.approval)
    elif args.phase=='restore':
        result=restore(args.source,args.output)
    else:
        result=verify_delta(args.source,args.output)
    print(json.dumps(result,ensure_ascii=False),flush=True)


if __name__=='__main__':
    main()
