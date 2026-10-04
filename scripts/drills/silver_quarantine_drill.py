"""불량 레코드를 일부러 넣어 Silver가 그것을 격리하는지 본다.

31일치 1억 5,571만 행에서 격리가 0건이었다. 그래서 격리 경로는 실데이터로 한 번도
돈 적이 없다. 0건이 "규칙이 맞다"인지 "규칙이 안 돈다"인지는 걸릴 행을 넣어 봐야 가른다.

**운영 테이블은 읽기만 한다.** Bronze의 하루치 파티션 하나를 임시 자리에 복사해,
같은 사본으로 Silver를 두 번 만든다. 불량을 넣기 전과 넣은 뒤다.

넣는 불량은 넷이다(flux.silver.quality.spoiled). 성한 레코드 하나에서 한 곳씩만 고친다.

    사용자 이름이 빈 것, 청취 시각이 1970 이전인 것, 적재 시각보다 미래인 것,
    track_metadata가 객체가 아니라 문자열인 것

확인하는 것.

    넣기 전 격리 0, 넣은 뒤 격리 4
    넷의 사유가 순수 판정(quality.judge)이 낸 사유와 같다
    넷이 Silver에 없다, Silver 행 수가 넣기 전과 같다
    격리 표의 raw_json이 넣은 문자열과 한 글자도 다르지 않다
    운영 Bronze, Silver, 격리 표에 새 커밋이 없다

    ./scripts/spark_run.sh -m 10g -- python scripts/drills/silver_quarantine_drill.py
"""

import argparse
import json
import shutil
import sys
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config, deltalog
from flux.common.spark import session as spark_session
from flux.silver import build, quality, schema

DRILL = "quarantine-drill"
MARK = "quarantine-drill"


def latest_version(table) -> int:
    return max(int(c.stem) for c in deltalog.commits(table))


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None,
                        help="복사할 Bronze 적재 날짜. 기본은 가장 이른 날")
    parser.add_argument("--keep", action="store_true",
                        help="끝나도 사본을 지우지 않는다. 조사할 때만 쓴다")
    args = parser.parse_args()

    root = config.local_root()
    production = {
        "bronze": root / config.BRONZE / bronze_schema.TABLE_NAME,
        "silver": root / config.SILVER / schema.TABLE_NAME,
        "quarantine": root / config.SILVER / schema.QUARANTINE_TABLE_NAME,
    }
    versions_before = {k: latest_version(v) for k, v in production.items()}

    day = args.date or min(
        v[bronze_schema.PARTITION_COLUMN]
        for v in deltalog.partition_values(production["bronze"]).values())

    work_dir = root / config.SCRATCH / DRILL
    work_bronze = config.scratch(DRILL, bronze_schema.TABLE_NAME)
    work_silver = config.scratch(DRILL, schema.TABLE_NAME)
    work_quarantine = config.scratch(DRILL, schema.QUARANTINE_TABLE_NAME)
    built_at = datetime.now(timezone.utc)

    spark = spark_session("flux-quarantine-drill", shuffle_partitions=32, memory="6g")
    try:
        (spark.read.format("delta").load(config.bronze(bronze_schema.TABLE_NAME))
         .filter(F.col(bronze_schema.PARTITION_COLUMN) == day)
         .write.format("delta").mode("overwrite")
         .partitionBy(bronze_schema.PARTITION_COLUMN).save(work_bronze))
        copied = spark.read.format("delta").load(work_bronze)
        copied_rows = copied.count()
        print(f"복사한 날   : {day}  ({copied_rows:,}행)")
        print(f"사본        : {work_bronze}")

        silver_before, rejected_before = build.build(spark, work_bronze,
                                                     built_at=built_at)
        silver_rows_before = silver_before.count()
        rejected_rows_before = rejected_before.count()
        print(f"넣기 전     : Silver {silver_rows_before:,}, 격리 {rejected_rows_before:,}")

        good = (build.with_quality(copied).filter(F.col("_reason").isNull())
                .withColumn("_epoch", F.unix_timestamp("_ingested_at"))
                .orderBy("_record_id").first())
        next_id = copied.agg(F.max("_record_id")).first()[0] + 1
        samples = quality.spoiled(json.loads(good["raw_json"]),
                                  ingested_at=good["_epoch"])

        injected = []
        for offset, sample in enumerate(samples):
            raw = json.dumps(sample.document, ensure_ascii=False)
            injected.append({
                "_record_id": next_id + offset,
                "label": sample.label,
                "reason": sample.reason,
                "raw_json": raw,
                "row": Row(**{
                    **good.asDict(),
                    "_record_id": next_id + offset,
                    "_source_file": MARK,
                    "listened_at": quality.listened_at_of(sample.document),
                    "user_name": sample.document.get("user_name"),
                    "raw_json": raw,
                }),
            })
        columns = bronze_schema.column_names()
        rows = [tuple(item["row"][c] for c in columns) for item in injected]
        (spark.createDataFrame(rows, copied.select(*columns).schema)
         .write.format("delta").mode("append").save(work_bronze))
        print(f"넣은 불량   : {len(injected)}행 (_record_id {next_id}~{next_id + len(injected) - 1})")

        silver_after, rejected_after = build.build(spark, work_bronze,
                                                   built_at=built_at)
        build.write(silver_after, work_silver, partition=schema.PARTITION_COLUMN)
        build.write(rejected_after, work_quarantine,
                    partition=schema.QUARANTINE_PARTITION_COLUMN)
        silver = spark.read.format("delta").load(work_silver)
        quarantined = spark.read.format("delta").load(work_quarantine)
        silver_rows_after = silver.count()

        ids = [item["_record_id"] for item in injected]
        arrived = {r["_record_id"]: r for r in
                   quarantined.filter(F.col("_record_id").isin(ids)).collect()}
        leaked = silver.filter((F.col("_record_id").isin(ids))
                               & (F.col("_source_dump") == good["_source_dump"])).count()

        print()
        print("넣은 것과 도착한 것")
        for item in injected:
            got = arrived.get(item["_record_id"])
            print(f"  {item['label']:14} 기대 {item['reason']:22} "
                  f"도착 {got['reason'] if got else '없음'}")

        versions_after = {k: latest_version(v) for k, v in production.items()}
        checks = {
            "넣기 전 격리는 0이다": rejected_rows_before == 0,
            "넣은 뒤 격리는 넷이다": quarantined.count() == len(injected),
            "넷이 전부 격리 표에 있다": set(arrived) == set(ids),
            "사유가 순수 판정과 같다": all(
                arrived.get(i["_record_id"]) is not None
                and arrived[i["_record_id"]]["reason"] == i["reason"]
                for i in injected),
            "Silver에는 없다": leaked == 0,
            "Silver 행 수가 그대로다": silver_rows_after == silver_rows_before,
            "raw_json이 온전하다": all(
                arrived.get(i["_record_id"]) is not None
                and arrived[i["_record_id"]]["raw_json"] == i["raw_json"]
                for i in injected),
            "운영 테이블에 새 커밋이 없다": versions_after == versions_before,
        }
        print()
        print("확인")
        for label, ok in checks.items():
            print(f"  {label:24} {'통과' if ok else '실패'}")

        config.write_measurement("silver-quarantine-drill", {
            "day": day,
            "copied_rows": copied_rows,
            "silver_before": silver_rows_before,
            "silver_after": silver_rows_after,
            "quarantine_before": rejected_rows_before,
            "injected": [{k: v for k, v in item.items() if k != "row"}
                         for item in injected],
            "arrived": {str(k): v["reason"] for k, v in arrived.items()},
            "production_versions": versions_before,
            "checks": {k: bool(v) for k, v in checks.items()},
            "worked_on_copy": True,
        })

        failed = [k for k, v in checks.items() if not v]
        if failed:
            print()
            print(f"실패: {', '.join(failed)}")
            return 1
        print()
        print("격리 주입 통과")
        return 0
    finally:
        spark.stop()
        if not args.keep and work_dir.exists():
            shutil.rmtree(work_dir)


if __name__ == "__main__":
    sys.exit(main())
