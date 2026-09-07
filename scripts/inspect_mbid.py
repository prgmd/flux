"""MBID가 실제 레코드 어디에 어떤 모양으로 들어 있는지 조사한다.

명세 6.3의 우선순위 규칙은 `mbid_mapping`과 `additional_info.recording_mbid`가
있다고 전제한다. 그 전제가 맞는지, 두 자리의 값이 실제로 다른지를 본다.

같은 MBID라도 ListenBrainz가 해석해 붙인 값과 클라이언트가 그냥 제출한 값은
신뢰도가 다르다. 그 구분이 성립하려면 두 자리가 실제로 갈려 있어야 한다.

    docker run ... python scripts/inspect_mbid.py --root /data --sample 200000
"""

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE_DIR = "bronze/raw_listens"


def make_session():
    builder = (
        SparkSession.builder.appName("flux-inspect-mbid")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "64")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def walk(node, prefix, keys):
    """중첩 문서의 키 경로를 모은다. 리스트는 첫 원소만 대표로 본다."""
    if isinstance(node, dict):
        for key, value in node.items():
            path = f"{prefix}.{key}" if prefix else key
            keys.add(path)
            walk(value, path, keys)
    elif isinstance(node, list) and node:
        walk(node[0], f"{prefix}[]", keys)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True)
    parser.add_argument("--sample", type=int, default=200_000)
    args = parser.parse_args()

    root = Path(args.root)
    spark = make_session()
    try:
        bronze = spark.read.format("delta").load(str(root / BRONZE_DIR))
        total = bronze.count()

        # MBID가 들어올 수 있는 자리를 전부 센다. 명세는 두 곳만 보지만
        # 실제 레코드에는 클라이언트마다 다른 이름으로 들어온다.
        FIELDS = {
            "mbid_mapping.recording_mbid":
                "$.track_metadata.mbid_mapping.recording_mbid",
            "mbid_mapping.artist_mbids":
                "$.track_metadata.mbid_mapping.artist_mbids",
            "additional_info.recording_mbid":
                "$.track_metadata.additional_info.recording_mbid",
            "additional_info.lastfm_track_mbid":
                "$.track_metadata.additional_info.lastfm_track_mbid",
            "additional_info.artist_mbids":
                "$.track_metadata.additional_info.artist_mbids",
            "additional_info.lastfm_artist_mbid":
                "$.track_metadata.additional_info.lastfm_artist_mbid",
            "additional_info.release_mbid":
                "$.track_metadata.additional_info.release_mbid",
            "additional_info.lastfm_release_mbid":
                "$.track_metadata.additional_info.lastfm_release_mbid",
            "recording_msid": "$.recording_msid",
            "submission_client": "$.track_metadata.additional_info.submission_client",
        }

        # cache()를 쓰지 않는다. 1억 5571만 행 × 문자열 10컬럼은 힘에 안 들어간다.
        # 대신 필요한 값을 전부 하나의 집계로 모아 한 번만 훑는다.
        cols = {
            name: F.get_json_object("raw_json", path)
            for name, path in FIELDS.items()
        }
        mapping = cols["mbid_mapping.recording_mbid"]
        client = F.coalesce(
            cols["additional_info.recording_mbid"],
            cols["additional_info.lastfm_track_mbid"],
        )

        aggregations = [
            F.count(F.when(expr.isNotNull(), 1)).alias(name.replace(".", "__"))
            for name, expr in cols.items()
        ] + [
            F.count(F.when(mapping.isNotNull(), 1)).alias("src_MAPPING"),
            F.count(F.when(mapping.isNull() & client.isNotNull(), 1))
             .alias("src_CLIENT"),
            F.count(F.when(mapping.isNull() & client.isNull(), 1)).alias("src_NONE"),
            F.count(F.when(
                mapping.isNotNull() & cols["additional_info.recording_mbid"].isNotNull(),
                1)).alias("both"),
            F.count(F.when(
                mapping.isNotNull()
                & (mapping == cols["additional_info.recording_mbid"]), 1))
             .alias("both_same"),
        ]
        counts = bronze.agg(*aggregations).collect()[0]

        print(f"전체 행 : {total:,}")
        print()
        print("MBID가 들어올 수 있는 자리")
        field_counts = {}
        for name in FIELDS:
            n = counts[name.replace(".", "__")]
            field_counts[name] = n
            print(f"  {name:38} {n:>12,}  {n / total * 100:6.2f}%")

        dist = {k: counts[f"src_{k}"] for k in ("MAPPING", "CLIENT", "NONE")}
        print()
        print("우선순위 적용 결과 (recording MBID 기준)")
        for source in ("MAPPING", "CLIENT", "NONE"):
            n = dist[source]
            print(f"  {source:8} {n:>12,}  {n / total * 100:6.2f}%")

        both_n, same = counts["both"], counts["both_same"]
        print()
        print(f"두 자리에 모두 있는 행 : {both_n:,}")
        if both_n:
            print(f"  값이 같음 : {same:,} ({same / both_n * 100:.1f}%)")
            print(f"  값이 다름 : {both_n - same:,} "
                  f"({(both_n - same) / both_n * 100:.1f}%)")

        clients = (
            bronze.select(cols["submission_client"].alias("client"))
                  .groupBy("client").count()
                  .orderBy(F.col("count").desc()).limit(12).collect()
        )
        print()
        print("제출 클라이언트 상위 12개")
        for row in clients:
            name = row["client"] or "(없음)"
            print(f"  {name[:42]:44} {row['count']:>12,}  "
                  f"{row['count'] / total * 100:5.2f}%")

        sample = (
            bronze.select("raw_json").limit(args.sample).collect()
        )
        mapping_keys = Counter()
        examples = []
        for row in sample:
            doc = json.loads(row["raw_json"])
            mapping = doc.get("mbid_mapping") or \
                (doc.get("track_metadata") or {}).get("mbid_mapping")
            if not mapping:
                continue
            keys = set()
            walk(mapping, "", keys)
            mapping_keys.update(keys)
            if len(examples) < 2:
                examples.append(mapping)

        print()
        print(f"표본 {len(sample):,}행 중 mbid_mapping을 가진 행의 키")
        for key, n in mapping_keys.most_common(20):
            print(f"  {key:44} {n:>8,}")

        for i, example in enumerate(examples, 1):
            print(f"\n예시 {i}")
            print(json.dumps(example, ensure_ascii=False, indent=2)[:800])

        payload = {
            "total_rows": total,
            "field_counts": field_counts,
            "source_distribution": dist,
            "both": both_n,
            "both_same": same,
            "clients": {(r["client"] or "(없음)"): r["count"] for r in clients},
            "sample_size": len(sample),
            "mapping_keys": dict(mapping_keys.most_common(40)),
        }
        out = root / "measurements"
        out.mkdir(parents=True, exist_ok=True)
        (out / "mbid-shape.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
