"""스테이징의 jsonl을 Bronze Delta로 적재하고 원본과 대조한다.

컨테이너 안에서 spark-submit 없이 직접 돌린다(로컬 모드).

경로는 FLUX_STORAGE_ROOT에서 온다. Spark에 넘기는 경로는 config.bronze()가
스킴을 유지한 채 만들고, 스테이징과 기록은 로컬 파일이므로 local_root()를 쓴다.
직접 Path로 조립하면 안 된다. pathlib이 s3a:// 의 연속 슬래시를 접는다.

    FLUX_STORAGE_ROOT=file:///data python scripts/bronze_ingest.py --dump <이름>
"""

import argparse
import sys
import time
from datetime import datetime, timezone


from flux.bronze import ingest, merge, schema, verify
from flux.common import config
from flux.common.spark import session as spark_session


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", required=True, help="덤프 식별자")
    parser.add_argument("--shuffle-partitions", type=int, default=32)
    args = parser.parse_args()

    root = config.local_root()
    staging = root / "staging" / args.dump
    files = sorted(staging.glob("*.listens"))
    if not files:
        raise SystemExit(f"스테이징에 파일이 없다: {staging}")

    bronze = config.bronze(schema.TABLE_NAME)
    quarantine = config.bronze(schema.QUARANTINE_TABLE_NAME)
    ingested_at = datetime.now(timezone.utc)

    print(f"덤프      : {args.dump}")
    print(f"입력      : {len(files)}개 파일")
    source_lines = 0
    for f in files:
        n = verify.count_lines(f)
        source_lines += n
        print(f"  {f.name:24} {n:>12,}줄  {f.stat().st_size / 1024**3:.2f}GB")
    print(f"원본 합계 : {source_lines:,}줄\n")

    spark = spark_session("flux-bronze-ingest", shuffle_partitions=args.shuffle_partitions, memory="4g")
    started = time.time()
    try:
        frames = []
        for f in files:
            print(f"읽는 중: {f.name}", flush=True)
            frames.append(ingest.build_dataframe(
                spark, str(f),
                source_dump=args.dump, source_file=f.name, ingested_at=ingested_at,
            ))

        valid, rejected = ingest.split(merge.combine(frames))
        print(f"적재 중: {len(frames)}개 파일을 한 번에", flush=True)
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
        config.write_measurement(f"bronze-{args.dump}", payload)

        if not result.ok:
            print("\n대조 실패")
            return 1
        print("\n대조 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
