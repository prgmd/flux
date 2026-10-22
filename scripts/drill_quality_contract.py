import argparse
import json

from flux.validation.quality_drill import run_drill


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--baseline',required=True)
    parser.add_argument('--output',required=True)
    args=parser.parse_args()
    result=run_drill(args.baseline,args.output)
    print(json.dumps({k:result[k] for k in ['drill_passed','scenarios_checked','baseline_unchanged']}))
    return 0 if result['drill_passed'] else 1


if __name__=='__main__':raise SystemExit(main())
