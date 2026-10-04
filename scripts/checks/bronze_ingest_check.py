"""Bronze 적재를 작은 표본으로 끝에서 끝까지 확인한다.

컨테이너 안에서 돈다. 실제 덤프가 아니라 손으로 만든 표본을 쓰므로 빠르다.
멱등성(두 번 돌려도 같은가)과 무변형(원본 문자열이 그대로인가)을 함께 본다.
"""

import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


from flux.common.spark import session as spark_session
from flux.bronze import ingest, schema

AT = datetime(2026, 9, 6, 12, 0, 0, tzinfo=timezone.utc)
DUMP = "listenbrainz-dump-2651-20260904-000002-incremental"
FILE = "listens/2026/9.listens"

GOOD = [
    {"user_name": "Pfefan", "timestamp": 1788393521,
     "track_metadata": {"track_name": "Home To You", "additional_info": {"duration_ms": 202821}}},
    {"user_name": "김광민", "timestamp": 1108339087,
     "track_metadata": {"track_name": "Café del Mar"}},
    {"user_name": "x", "listened_at": 999,
     "track_metadata": {"additional_info": {"tags": ["a", "b"]}}},
]
BAD = ['{"broken": ', "", "   ", "[1,2,3]", "not json"]


def write_sample(directory: Path) -> list[str]:
    """원본 줄을 그대로 파일에 쓴다. 이 문자열이 raw_json과 같아야 한다."""
    lines = [json.dumps(d, ensure_ascii=False) for d in GOOD] + BAD
    path = directory / "sample.listens"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return lines


def load(spark, text_path, bronze_path, quarantine_path):
    df = ingest.build_dataframe(
        spark, str(text_path), source_dump=DUMP, source_file=FILE, ingested_at=AT
    )
    valid, rejected = ingest.split(df)
    ingest.write(valid, bronze_path, source_dump=DUMP)
    ingest.write(rejected, quarantine_path, source_dump=DUMP)
    return valid.count(), rejected.count()


def main() -> int:
    workdir = Path(tempfile.mkdtemp(prefix="flux-bronze-"))
    spark = None
    failures = []

    def check(label, actual, expected):
        ok = actual == expected
        print(f"  {'OK  ' if ok else '실패'} {label}: {actual}" + ("" if ok else f" (기대 {expected})"))
        if not ok:
            failures.append(label)

    try:
        spark = spark_session("flux-bronze-check")
        source_lines = write_sample(workdir)
        bronze = str(workdir / "bronze")
        quarantine = str(workdir / "quarantine")

        print("1회차 적재")
        good, bad = load(spark, workdir / "sample.listens", bronze, quarantine)
        check("유효 행", good, len(GOOD))
        check("격리 행", bad, len(BAD))

        table = spark.read.format("delta").load(bronze)
        check("Bronze 행 수", table.count(), len(GOOD))
        check("스키마 컬럼", table.columns, schema.column_names())

        print("무변형 확인")
        stored = [r["raw_json"] for r in table.orderBy("_record_id").collect()]
        for original in source_lines[: len(GOOD)]:
            check(f"원문 보존 {original[:28]}...", original in stored, True)

        print("파생 컬럼")
        rows = {r["user_name"]: r for r in table.collect()}
        check("timestamp에서 뽑기", rows["Pfefan"]["listened_at"], 1788393521)
        check("listened_at 우선", rows["x"]["listened_at"], 999)
        check("한글 사용자", "김광민" in rows, True)
        check("파티션은 적재일", str(rows["Pfefan"]["ingested_date"]), "2026-09-06")

        print("2회차 적재 (멱등성)")
        good2, bad2 = load(spark, workdir / "sample.listens", bronze, quarantine)
        again = spark.read.format("delta").load(bronze)
        check("행 수가 그대로", again.count(), len(GOOD))
        check("격리 행도 그대로", spark.read.format("delta").load(quarantine).count(), len(BAD))

        print("Delta 이력")
        history = spark.sql(f"DESCRIBE HISTORY delta.`{bronze}`")
        check("버전이 2개", history.count(), 2)

        print()
        if failures:
            print(f"실패 {len(failures)}건: {', '.join(failures)}")
            return 1
        print("Bronze 적재 확인 통과")
        return 0
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
