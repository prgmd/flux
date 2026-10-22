import argparse
import json
from pathlib import Path

from flux.validation.baseline import replay_sample
from flux.validation.pinning import verify_bundle


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--baseline',type=Path,required=True)
    args=parser.parse_args()
    info=verify_bundle(args.baseline)
    result=replay_sample(args.baseline)
    result.update(baseline_ready=info['baseline_ready'],monthly_rows=info['monthly_rows'],plays=info['plays'])
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
