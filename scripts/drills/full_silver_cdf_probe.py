import argparse
import json
from pathlib import Path

from flux.common.spark import session
from flux.validation.full_gold_cdf import produce_probe


parser=argparse.ArgumentParser()
parser.add_argument('--silver',type=Path,required=True)
parser.add_argument('--output',type=Path,required=True)
parser.add_argument('--changes',type=Path,required=True)
args=parser.parse_args()
spark=session('flux-full-silver-cdf-probe',master_url='local[2]',memory='8g',shuffle_partitions=16,
              extra={'spark.jars.ivy':'/opt/flux/ivy','spark.databricks.delta.snapshotPartitions':'4','spark.sql.adaptive.enabled':'true'})
spark.sparkContext.setLogLevel('ERROR')
try:
    result=produce_probe(spark,args.silver,args.output,args.changes)
    print(json.dumps({key:result.get(key) for key in ['completed','counts','cdf_actions','elapsed_seconds']},ensure_ascii=False),flush=True)
finally:
    spark.stop()
