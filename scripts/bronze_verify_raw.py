"""적재된 raw_json을 원본 파일과 문자열 단위로 대조한다.

무변형을 증명하는 방법은 하나뿐이다. **원본에서 직접 읽어 와 비교하는 것.**
Bronze만 들여다봐서는 그것이 원본과 같은지 알 수 없다.

441만 줄을 모두 비교할 수는 없으므로 표본을 뽑되, 고르는 방식이 중요하다.
앞쪽만 보면 파일 뒷부분의 문제를 놓치므로 파일 전체에 고르게 흩는다.
"""

import argparse
import json
import sys
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flux.common import config

BRONZE_TABLE = "raw_listens"


def make_session():
    builder = (
        SparkSession.builder.appName("flux-bronze-verify-raw")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def sample_source_lines(path: Path, step: int) -> dict[str, int]:
    """원본을 훑으며 step줄마다 하나씩 골라 둔다. 값은 줄 번호."""
    picked = {}
    with open(path, "r", encoding="utf-8", errors="replace") as fp:
        for i, line in enumerate(fp):
            if i % step == 0:
                picked[line.rstrip("\n")] = i
    return picked


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dump", required=True)
    parser.add_argument("--step", type=int, default=5000, help="원본에서 N줄마다 하나씩 표본")
    args = parser.parse_args()

    root = config.local_root()
    staging = root / "staging" / args.dump
    files = sorted(staging.glob("*.listens"))
    if not files:
        raise SystemExit(f"스테이징에 파일이 없다: {staging}")

    print(f"덤프: {args.dump}")
    print(f"표본: 원본 {args.step}줄마다 1개\n")

    expected = {}
    for f in files:
        picked = sample_source_lines(f, args.step)
        print(f"  {f.name}: {len(picked):,}개 표본")
        expected.update(picked)

    spark = make_session()
    try:
        bronze = spark.read.format("delta").load(config.bronze(BRONZE_TABLE)) \
                     .filter(F.col("_source_dump") == args.dump)

        keys = list(expected)
        found = set()
        # IN 절이 너무 길어지지 않게 나눠서 조회한다.
        batch = 2000
        for i in range(0, len(keys), batch):
            chunk = keys[i:i + batch]
            rows = bronze.filter(F.col("raw_json").isin(chunk)) \
                         .select("raw_json").collect()
            found.update(r["raw_json"] for r in rows)

        missing = [k for k in keys if k not in found]

        print(f"\n표본 {len(keys):,}개 중 일치 {len(found):,}개, 불일치 {len(missing):,}개")
        if missing:
            print("\n불일치 예시:")
            for line in missing[:3]:
                print(f"  원본 줄 {expected[line]}: {line[:120]}")
            return 1

        total = bronze.count()
        print(f"Bronze 전체 {total:,}행")
        print("\n무변형 확인 통과: 표본 전부가 원본 문자열과 정확히 일치")

        payload = {
            "dump": args.dump,
            "sample_step": args.step,
            "sampled": len(keys),
            "matched": len(found),
            "bronze_rows": total,
        }
        out = root / "measurements"
        out.mkdir(parents=True, exist_ok=True)
        (out / f"bronze-raw-verify-{args.dump}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
