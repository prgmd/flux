import argparse
from datetime import datetime, timezone

from flux.common.spark import session
from flux.silver.monthly_listens import refresh


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--memory", default="8g")
    parser.add_argument("--partitions", type=int, default=64)
    args = parser.parse_args()
    spark = session("flux-monthly-silver", memory=args.memory,
                    master_url="local[4]", shuffle_partitions=args.partitions)
    try:
        print(refresh(spark, args.bronze, args.output, built_at=datetime.now(timezone.utc)))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
