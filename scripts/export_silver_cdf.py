import argparse
import json
from pathlib import Path

from flux.common.spark import session
from flux.validation.silver_cdf_export import export


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cursor", type=Path)
    args = parser.parse_args()
    previous = json.loads(args.cursor.read_text(encoding="utf-8")) if args.cursor else None
    spark = session("flux-silver-cdf-export", master_url="local[2]", memory="2g", shuffle_partitions=2,
                    extra={"spark.jars.ivy": "/opt/flux/ivy", "spark.databricks.delta.snapshotPartitions": "2"})
    spark.sparkContext.setLogLevel("ERROR")
    try:
        report = export(spark, args.silver, args.output, previous=previous)
        print(json.dumps(report, ensure_ascii=False), flush=True)
        return 0 if report["completed"] else 1
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
