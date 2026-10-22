import argparse
from pathlib import Path

from flux.common.spark import session
from flux.validation.cdf_drill import recover,start


parser=argparse.ArgumentParser()
parser.add_argument('--root',required=True,type=Path)
parser.add_argument('--recover',action='store_true')
parser.add_argument('--confirmed-exit-code',type=int)
args=parser.parse_args()
events=args.root/'spark-events';events.mkdir(exist_ok=True)
spark=session('flux-monthly-cdf-probe',master_url='local[2]',memory='2g',shuffle_partitions=2,
              extra={'spark.jars.ivy':'/opt/flux/ivy','spark.databricks.delta.snapshotPartitions':'2',
                     'spark.sql.adaptive.enabled':'false','spark.eventLog.enabled':'true',
                     'spark.eventLog.dir':events.as_uri(),'spark.eventLog.compress':'false','spark.eventLog.rolling.enabled':'false'})
spark.sparkContext.setLogLevel('ERROR')
try:
    if args.recover:recover(spark,args.root,confirmed_exit_code=args.confirmed_exit_code)
    else:start(spark,args.root)
finally:spark.stop()
