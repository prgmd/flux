"""시점 조인이 실제로 "당시 이름"을 주는지 확인한다.

명세 S4 게이트이자 SCD Type 2를 만든 이유 그 자체다.

    과거 청취가 당시 이름으로 조회되는 것을 확인한다

개명을 주입하고, **개명 전후의 청취를 각각 조회해** 다른 이름이 나오는지 본다.
한쪽만 보면 부족하다. 조인이 통째로 망가져 늘 옛 이름만 주어도 "과거 청취가
옛 이름으로 나온다"는 통과하기 때문이다. **양쪽이 서로 달라야** 검증이 된다.

차원을 건드리므로 확인이 끝나면 시점 조회로 되돌린다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/gold_pit_join_drill.py
"""

import argparse
import sys

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import fact, fact_listens, scd2

NEW_NAME = "개명 확인용 이름"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true",
                        help="확인 후 되돌리지 않는다")
    parser.add_argument("--no-broadcast", action="store_true",
                        help="차원 조인에 브로드캐스트를 강제하지 않는다. AQE가 고르게 두고 재려고 쓴다")
    args = parser.parse_args()

    dim_path = config.gold(scd2.TABLE_NAME)
    fact_path = config.gold(fact.TABLE_NAME)

    spark = spark_session("flux-pit-drill", shuffle_partitions=64)
    try:
        table = DeltaTable.forPath(spark, dim_path)
        before_version = table.history(1).select("version").first()["version"]
        print(f"주입 전 Delta 버전 : {before_version}")

        f = spark.read.format("delta").load(fact_path)
        dim = spark.read.format("delta").load(dim_path)

        # 청취가 많고 기간이 넓은 아티스트를 고른다. 개명 시각을 사이에 두고
        # 앞뒤로 청취가 있어야 비교가 성립한다.
        target = (
            f.filter(F.col("artist_mbid").isNotNull())
             .groupBy("artist_mbid")
             .agg(F.count(F.lit(1)).alias("n"),
                  F.min("listened_at").alias("first"),
                  F.max("listened_at").alias("last"))
             .filter(F.col("n") >= 100)
             .orderBy(F.col("n").desc())
             .first()
        )
        if target is None:
            print("조건에 맞는 아티스트가 없다")
            return 1

        mbid = target["artist_mbid"]
        first, last = target["first"], target["last"]
        old_name = (
            dim.filter((F.col(scd2.BUSINESS_KEY) == mbid)
                       & F.col(scd2.IS_CURRENT))
               .select("name").first()["name"]
        )
        # 개명 시각을 청취 구간의 한가운데로 잡는다.
        split = spark.sql(
            f"SELECT date_format(timestamp'{first}' "
            f"+ (timestamp'{last}' - timestamp'{first}')/2, 'yyyy-MM-dd') AS d"
        ).first()["d"]

        print(f"대상 아티스트 : {mbid}")
        print(f"원래 이름     : {old_name}")
        print(f"청취 구간     : {first} ~ {last}  ({target['n']:,}건)")
        print(f"개명 시각     : {split}")
        print()

        # 현재 줄을 닫고 새 이름으로 새 줄을 연다. dim_artist.merge와 같은
        # 결과지만 여기서는 대상 한 명뿐이라 직접 쓴다.
        (
            table.alias("t")
            .merge(
                spark.createDataFrame([(mbid,)], [scd2.BUSINESS_KEY]).alias("s"),
                f"t.{scd2.BUSINESS_KEY} = s.{scd2.BUSINESS_KEY} "
                f"AND t.{scd2.IS_CURRENT} = true")
            .whenMatchedUpdate(set={scd2.VALID_TO: F.lit(split),
                                    scd2.IS_CURRENT: F.lit(False)})
            .execute()
        )
        # **읽는 버전을 못 박는다.** 위에서 만든 dim을 그대로 쓰면 안 된다.
        #
        # 데이터프레임은 결과가 아니라 계산 방법이라, write가 실제로 도는 시점에
        # Delta가 **최신 스냅샷을 다시 읽는다.** 그때는 방금 MERGE가 현재 줄을
        # 닫은 뒤이므로 is_current 필터에 아무것도 안 걸리고, 새 줄이 통째로
        # 안 들어간다. 그러면 개명 후 청취가 짝을 못 찾는다.
        #
        # 병합 직전 버전을 명시해 그때의 값을 가져온다.
        renamed = (
            spark.read.format("delta")
            .option("versionAsOf", before_version).load(dim_path)
            .filter((F.col(scd2.BUSINESS_KEY) == mbid) & F.col(scd2.IS_CURRENT))
            .withColumn("name", F.lit(NEW_NAME))
            .withColumn(scd2.VALID_FROM, F.lit(split))
            .withColumn(scd2.VALID_TO, F.lit(scd2.FAR_FUTURE))
            .withColumn(scd2.IS_CURRENT, F.lit(True))
        )
        added = renamed.count()
        if added != 1:
            print(f"새 줄이 {added}개다. 1개여야 한다.")
            return 1
        renamed.write.format("delta").mode("append").save(dim_path)

        # 개명 전후의 청취를 각각 조회한다.
        dim_now = spark.read.format("delta").load(dim_path)
        joined = fact_listens.with_artist_name(
            f.filter(F.col("artist_mbid") == mbid), dim_now,
            broadcast=not args.no_broadcast)

        listened_date = F.date_format(F.col("listened_at"), "yyyy-MM-dd")
        by_era = (
            joined.withColumn("_era",
                              F.when(listened_date < F.lit(split), "개명 전")
                               .otherwise("개명 후"))
                  .groupBy("_era", "artist_name").count()
                  .orderBy("_era", F.col("count").desc()).collect()
        )

        print("시점별로 조회한 이름")
        for row in by_era:
            print(f"  {row['_era']:8} {str(row['artist_name'])[:30]:32} "
                  f"{row['count']:>10,}")

        eras = {}
        for row in by_era:
            eras.setdefault(row["_era"], set()).add(row["artist_name"])

        total = joined.count()
        unmatched = joined.filter(F.col("artist_name").isNull()).count()

        checks = {
            "개명 전은 옛 이름이다": eras.get("개명 전") == {old_name},
            "개명 후는 새 이름이다": eras.get("개명 후") == {NEW_NAME},
            "두 이름이 서로 다르다": old_name != NEW_NAME,
            "짝 없는 청취가 없다": unmatched == 0,
            "청취가 늘거나 줄지 않았다": total == target["n"],
        }
        print()
        print("확인")
        for label, ok in checks.items():
            print(f"  {label:24} {'통과' if ok else '실패'}")

        config.write_measurement("pit-join-drill", {
            "artist_mbid": mbid,
            "old_name": old_name,
            "new_name": NEW_NAME,
            "split_at": split,
            "listens": target["n"],
            "joined_rows": total,
            "unmatched": unmatched,
            "names_before": sorted(str(n) for n in eras.get("개명 전", [])),
            "names_after": sorted(str(n) for n in eras.get("개명 후", [])),
            "checks": {k: bool(v) for k, v in checks.items()},
            "restored_to_version": None if args.keep else before_version,
        })

        if not args.keep:
            print()
            print(f"버전 {before_version}으로 되돌린다")
            (
                spark.read.format("delta")
                .option("versionAsOf", before_version).load(dim_path)
                .write.format("delta").mode("overwrite").save(dim_path)
            )

        failed = [k for k, v in checks.items() if not v]
        if failed:
            print()
            print(f"실패: {', '.join(failed)}")
            return 1
        print()
        print("시점 조인 통과")
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
