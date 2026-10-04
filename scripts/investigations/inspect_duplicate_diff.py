"""중복인데 내용이 다른 레코드가 무엇이 다른지 조사한다.

중복 키의 17.9%가 내용이 서로 다르다. 같으면 아무 쪽을 남겨도 되지만
다르면 어느 쪽을 남길지 정해야 하고, 그 규칙은 무엇이 다른지를 봐야 정할 수 있다.

    ./scripts/spark_run.sh -- python scripts/investigations/inspect_duplicate_diff.py --keys 300
"""

import argparse
import json
import sys
from collections import Counter, defaultdict

from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.common.spark import session as spark_session

KEY = ["user_name", "listened_at", "msid"]


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
    parser.add_argument("--seed", type=int, default=20260914,
                        help="표본 추출 난수 씨앗. 고정해야 재현된다")
    args = parser.parse_args()

    spark = spark_session("flux-duplicate-diff", shuffle_partitions=args.shuffle_partitions)
    try:
        bronze = spark.read.format("delta").load(config.bronze(bronze_schema.TABLE_NAME))
        keyed = bronze.select(
            F.col("user_name"),
            F.col("listened_at"),
            F.get_json_object("raw_json", "$.recording_msid").alias("msid"),
            F.col("_source_dump"),
            F.col("raw_json"),
            F.md5(F.col("raw_json")).alias("digest"),
        )

        # limit()만 쓰면 표본이 아니다. 먼저 끝난 셔플 파티션에서 앞의 N개를
        # 집어오므로 키 해시가 가까운 몇 사용자에 몰린다. 실제로 그 편향 때문에
        # "user_id가 73.5%에서 바뀐다"는 잘못된 결론이 나왔다(전수는 0.02%).
        # 무작위로 섞은 뒤 잘라야 전체를 대표한다. seed를 고정해 재현 가능하게 둔다.
        differing = (
            keyed.groupBy(*KEY)
                 .agg(F.countDistinct("digest").alias("shapes"))
                 .filter(F.col("shapes") > 1)
                 .orderBy(F.rand(args.seed))
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
        distinct_users = len({k[0] for k in by_key})
        print(f"키 개수   : {keys_seen:,}")
        print(f"사용자 수 : {distinct_users:,}  "
              f"(키당 {keys_seen / distinct_users:.1f}개)")
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
            "distinct_users": distinct_users,
            "seed": args.seed,
            "rows_inspected": len(rows),
            "later_richer": later_richer,
            "later_poorer": later_poorer,
            "same_size": same_size,
            "added_fields": dict(added_fields.most_common(40)),
            "removed_fields": dict(removed_fields.most_common(40)),
            "changed_fields": dict(changed_fields.most_common(40)),
        }
        config.write_measurement("duplicate-diff", payload)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
