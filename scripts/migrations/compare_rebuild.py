"""운영 루트와 재구축 루트의 표를 대조한다. 행 번호 말고는 바뀐 것이 없는지 본다.

rehash_record_id.py로 Bronze를 옮기고 그 위에 Silver, 팩트, 마트를 다시 만든 뒤에 돈다.
차원은 운영 것을 복사해 두고 팩트 잡이 자리를 만들게 둔다. 기대는 네 가지다.

  1. 행 수와, 번호를 뺀 내용 지문이 표마다 같다
  2. 번호를 넣은 지문은 번호를 가진 표에서만 다르다. 번호가 실제로 바뀌었다는 뜻이다
  3. 새 번호가 표 안에서 겹치지 않는다
  4. 새 번호가 층 사이에서 이어진다. Silver의 (덤프, 번호)는 모두 Bronze에 있고 팩트의
     것은 모두 Silver에 있다

실행마다 달라지는 컬럼(`_built_at`, 격리 날짜)은 지문에서 뺀다.

    ./scripts/spark_run.sh -m 12g -- python scripts/migrations/compare_rebuild.py \
        --rebuild-root file:///data/rebuild
"""

import argparse
import sys
import time

from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import fact, mart, scd2
from flux.gold.mart_build import fingerprint
from flux.silver import schema as silver_schema

RECORD_ID = "_record_id"
RUN_COLUMNS = {"_built_at", silver_schema.QUARANTINE_PARTITION_COLUMN}

BRONZE_RAW = (config.BRONZE, bronze_schema.TABLE_NAME)
SILVER = (config.SILVER, silver_schema.TABLE_NAME)
FACT = (config.GOLD, fact.TABLE_NAME)

TABLES = (
    (BRONZE_RAW, fact.LISTEN_KEY),
    ((config.BRONZE, bronze_schema.QUARANTINE_TABLE_NAME), fact.LISTEN_KEY),
    (SILVER, fact.LISTEN_KEY),
    ((config.SILVER, silver_schema.QUARANTINE_TABLE_NAME), fact.LISTEN_KEY),
    ((config.GOLD, scd2.TABLE_NAME), None),
    (FACT, fact.GRAIN_KEY),
    ((config.GOLD, mart.DAILY_ARTIST), None),
)

LINKS = (
    (SILVER, BRONZE_RAW),
    ((config.SILVER, silver_schema.QUARANTINE_TABLE_NAME), BRONZE_RAW),
    (FACT, SILVER),
)


def summarize(df):
    run_free = [c for c in df.columns if c not in RUN_COLUMNS]
    content = [c for c in run_free if c != RECORD_ID]
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        fingerprint(*content).alias("content"),
        fingerprint(*run_free).alias("with_id"),
    ).first()
    return {"rows": row["rows"], "content": str(row["content"]), "with_id": str(row["with_id"])}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild-root", required=True)
    args = parser.parse_args()
    rebuild_root = args.rebuild_root.rstrip("/")

    def location(table, rebuilt):
        layer, name = table
        return f"{rebuild_root}/{layer}/{name}" if rebuilt else config.path(layer, name)

    spark = spark_session("flux-compare-rebuild", shuffle_partitions=200)

    def load(table, rebuilt):
        return spark.read.format("delta").load(location(table, rebuilt))

    failures = []
    payload = {"rebuild_root": rebuild_root, "tables": {}, "links": {}}
    try:
        for table, key in TABLES:
            started = time.time()
            before = summarize(load(table, rebuilt=False))
            after = summarize(load(table, rebuilt=True))
            duplicated = None
            if key:
                duplicated = (load(table, rebuilt=True).groupBy(*key).count()
                              .filter(F.col("count") > 1).count())
            same_rows = before["rows"] == after["rows"]
            same_content = before["content"] == after["content"]
            id_changed = before["with_id"] != after["with_id"]
            name = "/".join(table)
            print(f"{name:28} 행 {before['rows']:>13,} → {after['rows']:>13,}  "
                  f"내용 {'같음' if same_content else '다름'}  "
                  f"번호 {'바뀜' if id_changed else '그대로'}  "
                  f"겹친 키 {'-' if duplicated is None else f'{duplicated:,}'}  "
                  f"{time.time() - started:5.0f}초", flush=True)
            if not (same_rows and same_content) or duplicated:
                failures.append(name)
            payload["tables"][name] = {"before": before, "after": after,
                                       "duplicated_keys": duplicated}

        silver = load(SILVER, rebuilt=True)
        alone = silver.count() - silver.select(RECORD_ID).distinct().count()
        print(f"{'/'.join(SILVER)} 번호 하나만으로 겹친 행 {alone:,}  (참고, 키는 덤프와의 쌍)",
              flush=True)
        payload["silver_id_alone_overlaps"] = alone

        for child, parent in LINKS:
            started = time.time()
            orphans = (load(child, rebuilt=True).select(*fact.LISTEN_KEY)
                       .join(load(parent, rebuilt=True).select(*fact.LISTEN_KEY),
                             list(fact.LISTEN_KEY), "left_anti")
                       .count())
            name = f"{'/'.join(child)} → {'/'.join(parent)}"
            print(f"{name:52} 짝 없는 번호 {orphans:>11,}  {time.time() - started:5.0f}초",
                  flush=True)
            if orphans:
                failures.append(name)
            payload["links"][name] = orphans

        payload["failures"] = failures
        config.write_measurement("rebuild-compare", payload)
        if failures:
            print("어긋난 곳: " + ", ".join(failures))
            return 1
        print("행 번호 말고는 모두 같다")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
