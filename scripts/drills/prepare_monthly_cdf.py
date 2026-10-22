import argparse
import json

from flux.validation.cdf_inputs import prepare


parser=argparse.ArgumentParser()
parser.add_argument('--baseline',required=True)
parser.add_argument('--output',required=True)
parser.add_argument('--limit',type=int,default=128)
args=parser.parse_args()
result=prepare(args.baseline,args.output,limit=args.limit)
print(json.dumps({'prepared':result['prepared'],'sample_rows':[r['rows'] for r in result['archives']]}))
