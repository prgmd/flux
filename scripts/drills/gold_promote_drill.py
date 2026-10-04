"""placeholder 하나에 진짜 이름을 주입해, 이력을 만들지 않고 제자리에서 채우는지 본다.

차원에 없던 아티스트는 팩트가 이름 자리만 잡아 둔다(placeholder, 661개). 다음
MusicBrainz 릴리스에 그 아티스트가 들어오면 개명이 아니라 몰랐던 것을 알게 된
것이므로, 자리 줄을 닫지 않고 그대로 채워야 한다(flux.gold.scd2.stage의 stand_in).

확인하는 것은 넷이다.

    그 아티스트의 이력이 한 줄 그대로다
    그 줄이 여전히 1970부터 유효하다
    1970년 청취도 실제 청취도 새 이름으로 조회된다
    차원 전체 줄 수가 그대로이고 불변식이 깨지지 않는다

**운영 차원은 건드리지 않는다.** 차원을 임시 사본으로 복사해 거기에 주입하고, 끝나면
사본을 지운다. 처음에는 개명 드릴처럼 운영 차원에 주입하고 시점 조회로 되돌렸는데,
차원이 보름 넘게 안 바뀐 상태라 Delta가 보존 기간(7일)을 넘은 시점 조회를 막았다.
되돌리기가 실패하고 운영 차원이 채워진 채로 남았다. 그때는 로그로 옛 파일 목록을
만들어 그 줄만 되돌리고 테이블 전체의 해시 합이 옛 버전과 같은지 대조했다.

    ./scripts/spark_run.sh -m 10g -- python scripts/drills/gold_promote_drill.py
"""

import argparse
import shutil
import sys
from datetime import datetime

from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session
from flux.gold import dim_artist, fact, fact_listens, scd2

SILVER_TABLE = "mb_artist"
NEW_NAME = "승격 확인용 이름"
DRILL_DATE = "2026-12-26"
EARLIEST = datetime(1970, 1, 2, 12, 0)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep", action="store_true",
                        help="끝나도 사본을 지우지 않는다. 조사할 때만 쓴다")
    args = parser.parse_args()

    silver_path = config.silver(SILVER_TABLE)
    gold_path = config.gold(scd2.TABLE_NAME)
    fact_path = config.gold(fact.TABLE_NAME)

    work_path = config.scratch("promote-drill", scd2.TABLE_NAME)
    work_dir = config.local_root() / config.SCRATCH / "promote-drill"

    spark = spark_session("flux-promote-drill", shuffle_partitions=64, memory="6g")
    try:
        dim = spark.read.format("delta").load(gold_path)
        stand_ins = dim.filter(F.col(scd2.IS_CURRENT)
                               & (F.col("name") == fact.PLACEHOLDER_NAME))
        stand_ins_before = stand_ins.count()
        mbid = (stand_ins.orderBy(scd2.BUSINESS_KEY)
                .select(scd2.BUSINESS_KEY).first()[scd2.BUSINESS_KEY])
        rows_before = dim.count()
        print(f"placeholder       : {stand_ins_before:,}개")
        print(f"대상 아티스트      : {mbid}")
        print(f"채울 이름          : {NEW_NAME}")
        print(f"사본               : {work_path}")
        print()
        dim.write.format("delta").mode("overwrite").save(work_path)

        silver = spark.read.format("delta").load(silver_path)
        release = silver.select("_release").first()["_release"]
        if silver.filter(F.col(scd2.BUSINESS_KEY) == mbid).limit(1).count():
            print("대상이 이미 Silver에 있다. placeholder가 아니다")
            return 1

        arrived = (
            silver.limit(1)
            .withColumn(scd2.BUSINESS_KEY, F.lit(mbid))
            .withColumn("name", F.lit(NEW_NAME))
            .withColumn("sort_name", F.lit(NEW_NAME))
        )
        prepared = dim_artist.prepare(silver.unionByName(arrived),
                                      at=DRILL_DATE, release=release)
        dim_artist.merge(spark, work_path, prepared, at=DRILL_DATE)

        after = spark.read.format("delta").load(work_path)
        rows_after = after.count()
        stand_ins_after = after.filter(
            F.col(scd2.IS_CURRENT) & (F.col("name") == fact.PLACEHOLDER_NAME)).count()
        history = (
            after.filter(F.col(scd2.BUSINESS_KEY) == mbid)
            .select("name", scd2.VALID_FROM, scd2.VALID_TO, scd2.IS_CURRENT,
                    "_release")
            .collect()
        )
        print("그 아티스트의 이력")
        for row in history:
            state = "현재" if row[scd2.IS_CURRENT] else "닫힘"
            print(f"  {row['name'][:30]:32} "
                  f"{row[scd2.VALID_FROM]} ~ {row[scd2.VALID_TO]}  {state}")

        early = spark.createDataFrame(
            [(0, mbid, EARLIEST)],
            "listen_id long, artist_mbid string, listened_at timestamp")
        early_name = fact_listens.with_artist_name(early, after).first()["artist_name"]

        real = (spark.read.format("delta").load(fact_path)
                .filter(F.col(scd2.BUSINESS_KEY) == mbid)
                .select("artist_mbid", "listened_at"))
        named = fact_listens.with_artist_name(real, after)
        real_total = named.count()
        real_named = named.filter(F.col("artist_name") == NEW_NAME).count()
        print()
        print(f"1970-01-02 청취의 이름 : {early_name}")
        print(f"실제 청취              : {real_total:,}건 중 새 이름 {real_named:,}건")

        only = history[0] if len(history) == 1 else None
        checks = {
            "이력이 한 줄 그대로다": len(history) == 1,
            "그 줄이 새 이름이다": bool(only) and only["name"] == NEW_NAME,
            "그 줄이 1970부터 유효하다": bool(only) and
                only[scd2.VALID_FROM] == scd2.FAR_PAST,
            "그 줄이 열려 있다": bool(only) and only[scd2.IS_CURRENT] and
                only[scd2.VALID_TO] == scd2.FAR_FUTURE,
            "1970년 청취가 새 이름이다": early_name == NEW_NAME,
            "실제 청취가 전부 새 이름이다": real_total > 0 and real_named == real_total,
            "차원 줄 수가 그대로다": rows_after == rows_before,
            "placeholder가 하나 줄었다": stand_ins_after == stand_ins_before - 1,
        }
        production = spark.read.format("delta").load(gold_path)
        untouched = production.filter(F.col(scd2.BUSINESS_KEY) == mbid).first()
        checks["운영 차원은 그대로다"] = (
            production.count() == rows_before
            and fact.is_placeholder(untouched["name"]))
        print()
        print("확인")
        for label, ok in checks.items():
            print(f"  {label:24} {'통과' if ok else '실패'}")

        invariants = dim_artist.invariants(spark, work_path)
        print()
        print("SCD2 불변식")
        for name, value in invariants.items():
            print(f"  {name:38} {'통과' if value == 0 else f'위반 {value:,}'}")

        config.write_measurement("dim-artist-promote-drill", {
            "artist_mbid": mbid,
            "new_name": NEW_NAME,
            "drill_date": DRILL_DATE,
            "rows_before": rows_before,
            "rows_after": rows_after,
            "stand_ins_before": stand_ins_before,
            "stand_ins_after": stand_ins_after,
            "history": [
                {"name": r["name"], "valid_from": r[scd2.VALID_FROM],
                 "valid_to": r[scd2.VALID_TO], "is_current": r[scd2.IS_CURRENT],
                 "release": r["_release"]}
                for r in history
            ],
            "earliest_listen_name": early_name,
            "real_listens": real_total,
            "real_listens_named": real_named,
            "checks": {k: bool(v) for k, v in checks.items()},
            "invariants": invariants,
            "worked_on_copy": True,
        })

        failed = [k for k, v in checks.items() if not v]
        if failed or any(invariants.values()):
            print()
            print(f"실패: {', '.join(failed) or '불변식 위반'}")
            return 1
        print()
        print("placeholder 승격 통과")
        return 0
    finally:
        spark.stop()
        if not args.keep and work_dir.exists():
            shutil.rmtree(work_dir)


if __name__ == "__main__":
    sys.exit(main())
