import argparse
import sys

from flux.common import config, job_runs
from flux.common.spark import session as spark_session
from flux.silver import schema, user_recording_counts as counts


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    parser.add_argument("--driver-memory", default="8g")
    args = parser.parse_args()
    spark = spark_session("flux-silver-user-recording-counts",
                          shuffle_partitions=args.shuffle_partitions,
                          memory=args.driver_memory)
    try:
        version = counts.refresh(spark, config.silver(schema.TABLE_NAME),
                                 config.silver(counts.TABLE_NAME))
        print(f"사용자별 곡 청취 횟수 갱신 완료: Silver 버전 {version}")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(job_runs.run(main))
