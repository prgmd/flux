"""아티스트 개명을 손으로 주입해 SCD2가 이력을 쌓는지 확인한다.

명세 게이트가 요구하는 항목이다.

    아티스트 개명 시나리오를 수동으로 주입해 이력 2행이 생기고
    과거 청취가 당시 이름으로 조회되는 것을 확인한다.

실제 개명을 기다릴 수는 없다. 다음 릴리스에 그런 아티스트가 있을지도 모르고,
있어도 몇 달 뒤다. **일어날 일을 만들어 넣어 보는 것**이 유일한 방법이다.

차원을 건드리므로 확인이 끝나면 **원래대로 되돌린다.** 되돌리는 방법은
Delta의 시점 조회다 — 주입 직전 버전으로 복원한다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/gold_rename_drill.py
"""

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import dim_artist, scd2

SILVER_TABLE = "mb_artist"
NEW_NAME = "개명 확인용 이름"
DRILL_DATE = "2026-12-25"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true",
                        help="확인 후 되돌리지 않는다. 조사할 때만 쓴다")
    args = parser.parse_args()

    silver_path = config.silver(SILVER_TABLE)
    gold_path = config.gold(scd2.TABLE_NAME)

    spark = spark_session("flux-rename-drill", shuffle_partitions=64, memory="6g")
    try:
        table = DeltaTable.forPath(spark, gold_path)
        before_version = table.history(1).select("version").first()["version"]
        print(f"주입 전 Delta 버전 : {before_version}")

        dim = spark.read.format("delta").load(gold_path)
        target = (
            dim.filter(F.col(scd2.IS_CURRENT) & F.col("name").isNotNull())
               .orderBy(scd2.BUSINESS_KEY)
               .select(scd2.BUSINESS_KEY, "name").first()
        )
        mbid, old_name = target[scd2.BUSINESS_KEY], target["name"]
        print(f"대상 아티스트      : {mbid}")
        print(f"원래 이름          : {old_name}")
        print(f"바꿀 이름          : {NEW_NAME}")
        print()

        silver = spark.read.format("delta").load(silver_path)
        release = silver.select("_release").first()["_release"]

        # 그 아티스트의 이름만 바꾼 Silver를 만든다. 원본은 건드리지 않는다.
        renamed = silver.withColumn(
            "name",
            F.when(F.col(scd2.BUSINESS_KEY) == mbid, F.lit(NEW_NAME))
             .otherwise(F.col("name")),
        )
        prepared = dim_artist.prepare(renamed, at=DRILL_DATE, release=release)

        rows_before = dim.count()
        dim_artist.merge(spark, gold_path, prepared, at=DRILL_DATE)
        after = spark.read.format("delta").load(gold_path)
        rows_after = after.count()

        print(f"이력 줄 : {rows_before:,} → {rows_after:,}  "
              f"(늘어난 줄 {rows_after - rows_before:,})")

        history = (
            after.filter(F.col(scd2.BUSINESS_KEY) == mbid)
                 .select("name", scd2.VALID_FROM, scd2.VALID_TO, scd2.IS_CURRENT)
                 .orderBy(scd2.VALID_FROM).collect()
        )
        print()
        print("그 아티스트의 이력")
        for row in history:
            state = "현재" if row[scd2.IS_CURRENT] else "닫힘"
            print(f"  {row['name'][:30]:32} "
                  f"{row[scd2.VALID_FROM]} ~ {row[scd2.VALID_TO]}  {state}")

        checks = {
            "이력이 2행이 됐다": len(history) == 2,
            "옛 이름이 남아 있다": any(r["name"] == old_name for r in history),
            "새 이름이 현재다": any(
                r["name"] == NEW_NAME and r[scd2.IS_CURRENT] for r in history),
            "현재는 하나뿐이다": sum(
                1 for r in history if r[scd2.IS_CURRENT]) == 1,
            "구간이 이어진다": len(history) == 2 and
                history[0][scd2.VALID_TO] == history[1][scd2.VALID_FROM],
            "다른 아티스트는 그대로다": rows_after - rows_before == 1,
        }
        print()
        print("확인")
        for label, ok in checks.items():
            print(f"  {label:22} {'통과' if ok else '실패'}")

        invariants = dim_artist.invariants(spark, gold_path)
        print()
        print("SCD2 불변식")
        for name, value in invariants.items():
            print(f"  {name:38} {'통과' if value == 0 else f'위반 {value:,}'}")

        config.write_measurement("dim-artist-rename-drill", {
            "artist_mbid": mbid,
            "old_name": old_name,
            "new_name": NEW_NAME,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "history": [
                {"name": r["name"], "valid_from": r[scd2.VALID_FROM],
                 "valid_to": r[scd2.VALID_TO], "is_current": r[scd2.IS_CURRENT]}
                for r in history
            ],
            "checks": {k: bool(v) for k, v in checks.items()},
            "invariants": invariants,
            "restored_to_version": None if args.keep else before_version,
        })

        if not args.keep:
            # 시점 조회로 주입 전 상태를 읽어 통째로 덮어쓴다.
            # Delta가 옛 버전을 지우지 않기 때문에 가능한 복원이다.
            print()
            print(f"버전 {before_version}으로 되돌린다")
            (
                spark.read.format("delta")
                .option("versionAsOf", before_version).load(gold_path)
                .write.format("delta").mode("overwrite").save(gold_path)
            )
            restored = spark.read.format("delta").load(gold_path)
            print(f"복원 후 이력 줄 : {restored.count():,}")
            if restored.count() != rows_before:
                print("복원이 어긋났다")
                return 1

        failed = [k for k, v in checks.items() if not v]
        if failed or any(invariants.values()):
            print()
            print(f"실패: {', '.join(failed) or '불변식 위반'}")
            return 1
        print()
        print("개명 시나리오 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
