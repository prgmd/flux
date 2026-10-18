import argparse
import json

from flux.common import config, job_runs
from flux.common.spark import session
from flux.gold import artist_periods_build, fact, mart


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fact-path")
    parser.add_argument("--output")
    parser.add_argument("--fact-version", type=int)
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    args = parser.parse_args()
    if args.fact_version is not None and args.fact_version < 0:
        parser.error("--fact-version must be nonnegative")
    spark = session("flux-gold-artist-periods", shuffle_partitions=args.shuffle_partitions)
    try:
        metadata = artist_periods_build.refresh(
            spark, args.fact_path or config.gold(fact.TABLE_NAME),
            args.output or config.gold(mart.PERIOD_ARTIST), fact_version=args.fact_version)
        print(json.dumps(metadata, ensure_ascii=False))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(job_runs.run(main))
