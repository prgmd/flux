import argparse
import json
from pathlib import Path

from flux.validation.gx_drill import drill


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--validated',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();result=drill(args.validated,args.output)
    print(json.dumps(result,ensure_ascii=False))
    return 0 if result['completed'] else 1


if __name__=='__main__':raise SystemExit(main())
