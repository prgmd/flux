import argparse
import json
from pathlib import Path

from flux.common.spark import session
from flux.validation.gold_cdf_drill import produce


parser = argparse.ArgumentParser()
parser.add_argument("--source", type=Path, required=True)
parser.add_argument("--root", type=Path, required=True)
args = parser.parse_args()
spark = session("flux-gold-cdf-source", master_url="local[2]", memory="2g", shuffle_partitions=2,
                extra={"spark.jars.ivy": "/opt/flux/ivy", "spark.databricks.delta.snapshotPartitions": "2",
                       "spark.sql.adaptive.enabled": "false"})
spark.sparkContext.setLogLevel("ERROR")
try:
    result = produce(spark, args.source, args.root)
    print(json.dumps({key: result[key] for key in ["completed", "original_unchanged", "cdf_actions"]}), flush=True)
finally:
    spark.stop()
