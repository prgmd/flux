"""Bronze의 `_record_id`를 줄 내용의 해시로 다시 계산해 다른 루트에 쓴다. 한 번 하는 이전이다.

`_record_id`는 원래 `monotonically_increasing_id()`였다. 잡 하나 안에서만 겹치지 않는
번호라 같은 덤프를 다시 적재하면 번호가 바뀌고, 팩트의 청취 식별자(덤프, 번호)가 통째로
바뀌어 백필이 성립하지 않았다. 적재 코드는 이제 저장되는 글자의 해시를 쓴다
(flux.bronze.ingest.build_dataframe).

이미 쌓인 31덤프는 원본을 다시 적재하지 않는다. 성한 줄의 번호는 `xxhash64(raw_json)`이고
`raw_json`이 Bronze에 그대로 있으므로 계산만 하면 된다. 격리 줄은 `xxhash64(raw_line)`이다.
덤프 하나(2640)를 새 코드로 다시 적재한 번호와 이 계산이 (번호, 줄) 쌍으로 차이 0건인 것을
먼저 확인했다.

운영 루트는 읽기만 한다. 결과는 `--out-root` 아래 같은 표 이름으로 쓴다. 대조한 뒤 표
단위로 바꿔 끼운다.

    ./scripts/spark_run.sh -m 10g -- python scripts/migrations/rehash_record_id.py \
        --out-root file:///data/rebuild [--dump <덤프 하나만, 사전 확인용>]
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from flux.bronze import schema
from flux.common import config
from flux.common.spark import session as spark_session

TABLES = (
    (schema.TABLE_NAME, "raw_json", schema.COLUMNS),
    (schema.QUARANTINE_TABLE_NAME, "raw_line", schema.QUARANTINE_COLUMNS),
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-root", required=True)
    parser.add_argument("--dump", default=None)
    args = parser.parse_args()

    spark = spark_session("flux-rehash-record-id", shuffle_partitions=200)
    try:
        for table, text_column, columns in TABLES:
            started = time.time()
            source = spark.read.format("delta").load(config.bronze(table))
            if args.dump:
                source = source.filter(F.col("_source_dump") == args.dump)
            target = f"{args.out_root.rstrip('/')}/{config.BRONZE}/{table}"
            (source
             .withColumn("_record_id", F.xxhash64(F.col(text_column)))
             .select(*schema.column_names(columns))
             .write.format("delta").mode("overwrite")
             .partitionBy(schema.PARTITION_COLUMN)
             .save(target))
            written = spark.read.format("delta").load(target)
            rows = written.count()
            same = written.filter(F.col("_record_id") == F.xxhash64(F.col(text_column))).count()
            print(f"{table:16} {rows:>13,}행  해시와 같은 번호 {same:>13,}  "
                  f"{time.time() - started:6.0f}초  → {target}", flush=True)
            if same != rows:
                print("번호가 해시와 다른 행이 있다")
                return 1
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
