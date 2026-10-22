import argparse
import json
from pathlib import Path

from flux.validation.quality_contract import run


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--baseline',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--reference-profile',type=Path)
    args=parser.parse_args()
    reference=json.loads(args.reference_profile.read_text(encoding='utf-8')) if args.reference_profile else None
    result=run(args.baseline,args.output,reference)
    print(json.dumps({k:result[k] for k in ['quality_passed','promotion_ready','summary']},ensure_ascii=False))
    return 0 if result['quality_passed'] else 1


if __name__=='__main__':raise SystemExit(main())
