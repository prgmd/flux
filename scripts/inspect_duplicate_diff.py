"""중복인데 내용이 다른 레코드가 무엇이 다른지 조사한다.

중복 키의 17.9%가 내용이 서로 다르다. 같으면 아무 쪽을 남겨도 되지만
다르면 어느 쪽을 남길지 정해야 하고, 그 규칙은 무엇이 다른지를 봐야 정할 수 있다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/inspect_duplicate_diff.py --keys 300
"""

import argparse
import json
import sys
from collections import Counter, defaultdict

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from flux.common import config

BRONZE_TABLE = "raw_listens"
KEY = ["user_name", "listened_at", "msid"]


def make_session(shuffle_partitions: int):
    builder = (
        SparkSession.builder.appName("flux-duplicate-diff")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "8g")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def flatten(node, prefix=""):
    """중첩 문서를 경로 → 값의 평평한 사전으로 만든다."""
    out = {}
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{prefix}.{key}" if prefix else key
            if isinstance(value, (dict, list)):
                out.update(flatten(value, path))
            else:
                out[path] = value
    elif isinstance(node, list):
        out[prefix] = json.dumps(node, ensure_ascii=False, sort_keys=True)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keys", type=int, default=300, help="조사할 키 개수")
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    args = parser.parse_args()

    spark = make_session(args.shuffle_partitions)
    try:
        bronze = spark.read.format("delta").load(config.bronze(BRONZE_TABLE))
        keyed = bronze.select(
            F.col("user_name"),
            F.col("listened_at"),
            F.get_json_object("raw_json", "$.recording_msid").alias("msid"),
            F.col("_source_dump"),
            F.col("raw_json"),
            F.md5(F.col("raw_json")).alias("digest"),
        )

        differing = (
            keyed.groupBy(*KEY)
                 .agg(F.countDistinct("digest").alias("shapes"))
                 .filter(F.col("shapes") > 1)
                 .limit(args.keys)
                 .select(*KEY)
        )

        rows = (
            keyed.join(F.broadcast(differing), on=KEY, how="inner")
                 .select(*KEY, "_source_dump", "raw_json")
                 .collect()
        )
        print(f"조사 대상 : {len(rows):,}행")

        by_key = defaultdict(list)
        for row in rows:
            by_key[(row["user_name"], row["listened_at"], row["msid"])].append(
                (row["_source_dump"], row["raw_json"])
            )

        changed_fields = Counter()
        added_fields = Counter()
        removed_fields = Counter()
        later_richer = 0
        later_poorer = 0
        same_size = 0
        examples = []

        for key, entries in by_key.items():
            entries.sort(key=lambda e: e[0])          # 덤프 이름이 곧 시간 순서
            first, last = entries[0], entries[-1]
            a, b = flatten(json.loads(first[1])), flatten(json.loads(last[1]))

            for field in set(a) | set(b):
                if field not in a:
                    added_fields[field] += 1
                elif field not in b:
                    removed_fields[field] += 1
                elif a[field] != b[field]:
                    changed_fields[field] += 1

            if len(b) > len(a):
                later_richer += 1
            elif len(b) < len(a):
                later_poorer += 1
            else:
                same_size += 1

            if len(examples) < 3 and (set(a) ^ set(b) or a != b):
                examples.append((key, first, last))

        keys_seen = len(by_key)
        print(f"키 개수   : {keys_seen:,}")
        print()
        print("나중 덤프가 필드를 더 갖는가")
        for label, n in (("더 많다", later_richer), ("더 적다", later_poorer),
                         ("같다", same_size)):
            print(f"  {label:8} {n:>6,}  {n / keys_seen * 100:5.1f}%")

        for title, counter in (("나중에 생긴 필드", added_fields),
                               ("나중에 사라진 필드", removed_fields),
                               ("값이 바뀐 필드", changed_fields)):
            print()
            print(f"{title} (상위 12개)")
            if not counter:
                print("  없음")
            for field, n in counter.most_common(12):
                print(f"  {field:52} {n:>6,}")

        for i, (key, first, last) in enumerate(examples, 1):
            print()
            print(f"예시 {i}  user={key[0]} listened_at={key[1]}")
            print(f"  앞  {first[0][-30:]}")
            print(f"      {first[1][:400]}")
            print(f"  뒤  {last[0][-30:]}")
            print(f"      {last[1][:400]}")

        payload = {
            "keys_inspected": keys_seen,
            "rows_inspected": len(rows),
            "later_richer": later_richer,
            "later_poorer": later_poorer,
            "same_size": same_size,
            "added_fields": dict(added_fields.most_common(40)),
            "removed_fields": dict(removed_fields.most_common(40)),
            "changed_fields": dict(changed_fields.most_common(40)),
        }
        out = config.local_root() / "measurements"
        out.mkdir(parents=True, exist_ok=True)
        (out / "duplicate-diff.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
