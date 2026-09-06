"""스테이징의 jsonl을 Bronze Delta로 적재하고 원본과 대조한다.

컨테이너 안에서 spark-submit 없이 직접 돌린다(로컬 모드).
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from flux.bronze import ingest, schema, verify

BRONZE_DIR = "bronze/raw_listens"
QUARANTINE_DIR = "bronze/invalid_listens"


def make_session(shuffle_partitions: int):
    builder = (
        SparkSession.builder.appName("flux-bronze-ingest")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, help="데이터 루트 (컨테이너 안 경로)")
    parser.add_argument("--dump", required=True, help="덤프 식별자")
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    args = parser.parse_args()

    root = Path(args.root)
    staging = root / "staging" / args.dump
    files = sorted(staging.glob("*.listens"))
    if not files:
        raise SystemExit(f"스테이징에 파일이 없다: {staging}")

    bronze = str(root / BRONZE_DIR)
    quarantine = str(root / QUARANTINE_DIR)
    ingested_at = datetime.now(timezone.utc)

    print(f"덤프      : {args.dump}")
    print(f"입력      : {len(files)}개 파일")
    source_lines = 0
    for f in files:
        n = verify.count_lines(f)
        source_lines += n
        print(f"  {f.name:24} {n:>12,}줄  {f.stat().st_size / 1024**3:.2f}GB")
    print(f"원본 합계 : {source_lines:,}줄\n")

    spark = make_session(args.shuffle_partitions)
    started = time.time()
    try:
        for f in files:
            print(f"적재 중: {f.name}", flush=True)
            df = ingest.build_dataframe(
                spark, str(f),
                source_dump=args.dump, source_file=f.name, ingested_at=ingested_at,
            )
            valid, rejected = ingest.split(df)
            ingest.write(valid, bronze, source_dump=args.dump)
            ingest.write(rejected, quarantine, source_dump=args.dump)

        elapsed = time.time() - started
        bronze_rows = verify.count_rows(spark, bronze, args.dump)
        quarantine_rows = verify.count_rows(spark, quarantine, args.dump)
        result = verify.reconcile(source_lines, bronze_rows, quarantine_rows)

        print(f"\n적재 시간 : {elapsed:.0f}초")
        print(f"대조      : {result.explain()}")
        print(f"격리 비율 : {result.quarantine_ratio:.6f}")

        payload = {
            "dump": args.dump,
            "ingested_at": ingested_at.isoformat(),
            "elapsed_seconds": round(elapsed, 1),
            "source_lines": source_lines,
            "bronze_rows": bronze_rows,
            "quarantine_rows": quarantine_rows,
            "reconciled": result.ok,
            "quarantine_ratio": result.quarantine_ratio,
        }
        out = root / "measurements"
        out.mkdir(parents=True, exist_ok=True)
        (out / f"bronze-{args.dump}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

        if not result.ok:
            print("\n대조 실패")
            return 1
        print("\n대조 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
