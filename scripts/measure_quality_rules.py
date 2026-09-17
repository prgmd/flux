"""명세 6.3의 품질 규칙이 실제로 몇 건을 거르는지 잰다.

규칙은 넷이다.

    1. listened_at이 미래 또는 1970 이전      → 격리
    2. user_name / listened_at 누락            → 격리
    3. track_metadata 없음                     → 격리
    4. MBID가 UUID 형식이 아님                 → 해당 필드만 NULL, 레코드는 통과

앞의 셋은 0건일 가능성이 높다. 이미 키 컬럼의 빈 값이 0이었고
청취 시각도 2005~2026 안에 들어 있었다. 그래도 재는 이유가 있다.

**0이라는 사실 자체가 정보다.** 나중에 0이 아니게 되면 원천이 바뀌었다는 신호가 된다.
그리고 "그럴 것이다"와 "그렇다"는 다르다.

넷째는 아직 한 번도 재지 않았다. 필드가 있는지는 확인했지만
그 값이 UUID 형식인지는 보지 않았다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/measure_quality_rules.py
"""

import argparse
import sys

from pyspark.sql import functions as F

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.silver.mbid import UUID_PATTERN
from flux.common.spark import session as spark_session


# MBID가 들어올 수 있는 자리. mbid.resolve가 보는 곳과 같아야 한다.
MBID_FIELDS = {
    "mbid_mapping.recording_mbid":
        "$.track_metadata.mbid_mapping.recording_mbid",
    "additional_info.recording_mbid":
        "$.track_metadata.additional_info.recording_mbid",
    "additional_info.lastfm_track_mbid":
        "$.track_metadata.additional_info.lastfm_track_mbid",
    "additional_info.lastfm_artist_mbid":
        "$.track_metadata.additional_info.lastfm_artist_mbid",
    "additional_info.release_mbid":
        "$.track_metadata.additional_info.release_mbid",
    "additional_info.lastfm_release_mbid":
        "$.track_metadata.additional_info.lastfm_release_mbid",
}

EPOCH_1970 = 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=100)
    parser.add_argument("--show-malformed", type=int, default=0,
                        help="형식 위반 값을 몇 개까지 보여줄지")
    args = parser.parse_args()

    spark = spark_session("flux-quality-rules", shuffle_partitions=args.shuffle_partitions)
    try:
        bronze = spark.read.format("delta").load(config.bronze(bronze_schema.TABLE_NAME))

        listened = F.col("listened_at")
        # 미래는 그 행이 적재된 시각을 기준으로 본다. 지금 시각으로 재면
        # 오래 전에 적재한 행이 나중에 다시 재면 다르게 판정된다.
        ingested = F.unix_timestamp(F.col("_ingested_at"))

        metadata = F.get_json_object("raw_json", "$.track_metadata")
        track_name = F.get_json_object("raw_json", "$.track_metadata.track_name")
        artist_name = F.get_json_object("raw_json", "$.track_metadata.artist_name")

        checks = [
            F.count(F.lit(1)).alias("total"),
            # 규칙 2
            F.count(F.when(F.col("user_name").isNull(), 1)).alias("user_name_null"),
            F.count(F.when(F.trim(F.coalesce(F.col("user_name"), F.lit(""))) == "", 1))
             .alias("user_name_blank"),
            F.count(F.when(listened.isNull(), 1)).alias("listened_at_null"),
            # 규칙 1
            F.count(F.when(listened < EPOCH_1970, 1)).alias("before_1970"),
            F.count(F.when(listened == EPOCH_1970, 1)).alias("exactly_epoch"),
            F.count(F.when(listened > ingested, 1)).alias("future_vs_ingested"),
            F.min(listened).alias("min_listened_at"),
            F.max(listened).alias("max_listened_at"),
            # 규칙 3
            F.count(F.when(metadata.isNull(), 1)).alias("metadata_missing"),
            F.count(F.when(track_name.isNull(), 1)).alias("track_name_missing"),
            F.count(F.when(artist_name.isNull(), 1)).alias("artist_name_missing"),
            F.count(F.when(
                F.trim(F.coalesce(artist_name, F.lit(""))) == "", 1))
             .alias("artist_name_blank"),
        ]

        # 규칙 4. 값이 있는데 UUID 형식이 아닌 경우만 센다.
        for name, path in MBID_FIELDS.items():
            value = F.get_json_object("raw_json", path)
            alias = name.replace(".", "__")
            checks.append(
                F.count(F.when(value.isNotNull(), 1)).alias(f"{alias}__present"))
            checks.append(
                F.count(F.when(value.isNotNull() & ~value.rlike(UUID_PATTERN), 1))
                 .alias(f"{alias}__malformed"))

        row = bronze.agg(*checks).collect()[0]
        total = row["total"]

        print(f"전체 행 : {total:,}")
        print()
        print("규칙 1 — 청취 시각의 범위")
        for label, key in (("1970 이전", "before_1970"),
                           ("정확히 1970-01-01", "exactly_epoch"),
                           ("적재 시각보다 미래", "future_vs_ingested")):
            n = row[key]
            print(f"  {label:22} {n:>12,}  {n / total * 100:7.4f}%")
        print(f"  최소 {row['min_listened_at']:,}  최대 {row['max_listened_at']:,}")

        print()
        print("규칙 2 — 필수값 누락")
        for label, key in (("user_name 없음", "user_name_null"),
                           ("user_name 공백", "user_name_blank"),
                           ("listened_at 없음", "listened_at_null")):
            n = row[key]
            print(f"  {label:22} {n:>12,}  {n / total * 100:7.4f}%")

        print()
        print("규칙 3 — track_metadata")
        for label, key in (("track_metadata 없음", "metadata_missing"),
                           ("track_name 없음", "track_name_missing"),
                           ("artist_name 없음", "artist_name_missing"),
                           ("artist_name 공백", "artist_name_blank")):
            n = row[key]
            print(f"  {label:22} {n:>12,}  {n / total * 100:7.4f}%")

        print()
        print("규칙 4 — MBID 형식")
        mbid_stats = {}
        for name in MBID_FIELDS:
            alias = name.replace(".", "__")
            present = row[f"{alias}__present"]
            malformed = row[f"{alias}__malformed"]
            mbid_stats[name] = {"present": present, "malformed": malformed}
            share = malformed / present * 100 if present else 0.0
            print(f"  {name:36} 있음 {present:>11,}  "
                  f"형식 위반 {malformed:>8,}  ({share:.4f}%)")

        samples = {}
        if args.show_malformed:
            print()
            print("형식 위반 값의 실제 모습")
            for name, path in MBID_FIELDS.items():
                if not mbid_stats[name]["malformed"]:
                    continue
                value = F.get_json_object("raw_json", path)
                found = (
                    bronze.select(value.alias("value"))
                          .filter(F.col("value").isNotNull()
                                  & ~F.col("value").rlike(UUID_PATTERN))
                          .groupBy("value").count()
                          .orderBy(F.col("count").desc())
                          .limit(args.show_malformed).collect()
                )
                samples[name] = {r["value"]: r["count"] for r in found}
                print(f"  {name}")
                for r in found:
                    print(f"      {r['count']:>6,}회  {r['value']!r}")

        payload = {
            "total_rows": total,
            "rule_1_before_1970": row["before_1970"],
            "rule_1_exactly_epoch": row["exactly_epoch"],
            "rule_1_future": row["future_vs_ingested"],
            "listened_at_min": row["min_listened_at"],
            "listened_at_max": row["max_listened_at"],
            "rule_2_user_name_null": row["user_name_null"],
            "rule_2_user_name_blank": row["user_name_blank"],
            "rule_2_listened_at_null": row["listened_at_null"],
            "rule_3_metadata_missing": row["metadata_missing"],
            "rule_3_track_name_missing": row["track_name_missing"],
            "rule_3_artist_name_missing": row["artist_name_missing"],
            "rule_3_artist_name_blank": row["artist_name_blank"],
            "rule_4_mbid": mbid_stats,
            "rule_4_malformed_samples": samples,
        }
        config.write_measurement("quality-rules", payload)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
