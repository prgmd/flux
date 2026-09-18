"""Delta가 파일을 어떻게 두느냐로 조회가 얼마나 달라지는가.

명세 6.5의 세 줄을 잰다.

    Delta OPTIMIZE 전/후        파일 개수, 조회 시간
    Z-Order 적용 전/후          특정 아티스트 필터 쿼리 시간
    압축 코덱 snappy vs zstd     파일 크기, 읽기/쓰기 시간

## 작은 파일 문제

Spark는 파티션마다 태스크 수만큼 파일을 쓴다. 셔플 파티션이 200이면 한 달
구획에 파일이 200개 생길 수 있다. 데이터는 그대로인데 **파일만 잘게 쪼개진다.**

조회할 때마다 파일을 열고 메타데이터를 읽는 고정 비용이 붙으므로, 작은 파일이
많으면 그 비용이 데이터 읽는 시간을 넘어선다. `OPTIMIZE`는 작은 파일들을
큰 것으로 다시 쓴다.

## Z-Order가 다른 점

`OPTIMIZE`가 파일 **개수**를 줄인다면 Z-Order는 파일 **안의 순서**를 바꾼다.

Delta는 파일마다 각 컬럼의 최소값과 최대값을 로그에 적어 둔다. 조회 조건이
그 범위에 안 걸리면 **파일을 아예 열지 않는다**(data skipping). 같은 아티스트의
줄이 여러 파일에 흩어져 있으면 모든 파일의 범위에 걸려 건너뛸 것이 없다.
Z-Order는 그 줄들을 한데 모아 건너뛸 수 있게 만든다.

정렬 한 번의 대가로 이후의 모든 필터 조회가 싸진다. **쓰기를 비싸게 하고 읽기를
싸게 하는 거래**이고, 마트처럼 한 번 쓰고 여러 번 읽는 표에 맞는다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \\
        python scripts/experiment_delta.py
"""

import argparse
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from flux.common import config, deltalog
from flux.common.spark import session as spark_session
from flux.gold import fact, mart

CODECS = ("snappy", "zstd")


def folder_bytes(path: Path) -> int:
    return sum(p.stat().st_size for p in Path(path).rglob("*.parquet"))


def timed(fn):
    started = time.time()
    value = fn()
    return value, round(time.time() - started, 2)


def probe(spark, path: str, artist: str):
    """특정 아티스트만 거르는 조회. data skipping이 듣는지 보는 자리다."""
    return (
        spark.read.format("delta").load(path)
        .filter(F.col("artist_mbid") == artist)
        .count()
    )


def snapshot(spark, path: str, artist: str, label: str) -> dict:
    active = len(deltalog.active_files(path))
    size = folder_bytes(Path(path))
    _, full = timed(lambda: spark.read.format("delta").load(path).count())
    hits, filtered = timed(lambda: probe(spark, path, artist))
    row = {
        "label": label,
        "active_files": active,
        "bytes": size,
        "full_scan_seconds": full,
        "filter_seconds": filtered,
        "filter_hits": hits,
    }
    print(f"  {label:22} 파일 {active:>5}개  "
          f"{size / 1e6:>8.1f}MB  전수 {full:>6.2f}초  필터 {filtered:>6.2f}초")
    return row


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=200)
    args = parser.parse_args()

    mart_path = config.gold(mart.DAILY_ARTIST)
    root = config.local_root()

    spark = spark_session("flux-delta-experiment",
                          shuffle_partitions=args.shuffle_partitions)
    try:
        source = spark.read.format("delta").load(mart_path)
        # 필터에 쓸 아티스트는 가장 많이 나오는 것으로 고른다. 드문 값을 고르면
        # 어떤 배치에서도 파일 하나만 읽어 차이가 안 보인다.
        artist = (
            source.groupBy("artist_mbid").count()
                  .orderBy(F.col("count").desc()).first()["artist_mbid"]
        )
        print(f"필터 대상 아티스트 : {artist}")
        print()

        results = {}

        # 1) 압축 코덱. 같은 데이터를 코덱만 바꿔 쓴다.
        print("압축 코덱")
        for codec in CODECS:
            target = str(root / "experiments" / f"mart_{codec}")
            shutil.rmtree(target, ignore_errors=True)
            _, write = timed(lambda: (
                source.write.format("delta").mode("overwrite")
                .option("compression", codec).save(target)))
            row = snapshot(spark, target, artist, f"{codec} (쓰기 {write}초)")
            row["codec"] = codec
            row["write_seconds"] = write
            results[f"codec_{codec}"] = row

        # 2) OPTIMIZE 전후. 기본 코덱으로 다시 쓴 사본에 건다.
        print()
        print("OPTIMIZE")
        target = str(root / "experiments" / "mart_optimize")
        shutil.rmtree(target, ignore_errors=True)
        source.write.format("delta").mode("overwrite").save(target)
        results["optimize_before"] = snapshot(spark, target, artist, "전")

        _, took = timed(lambda: spark.sql(f"OPTIMIZE delta.`{target}`"))
        results["optimize_after"] = snapshot(
            spark, target, artist, f"후 ({took}초 걸림)")
        results["optimize_seconds"] = took

        # 3) Z-Order. OPTIMIZE된 것에 정렬을 더한다.
        print()
        print("Z-Order (artist_mbid)")
        _, took = timed(lambda: spark.sql(
            f"OPTIMIZE delta.`{target}` ZORDER BY (artist_mbid)"))
        results["zorder_after"] = snapshot(
            spark, target, artist, f"후 ({took}초 걸림)")
        results["zorder_seconds"] = took

        print()
        print("정리")
        before = results["optimize_before"]
        after = results["optimize_after"]
        zorder = results["zorder_after"]
        print(f"  파일 개수   {before['active_files']:>5} → "
              f"{after['active_files']:>5} (OPTIMIZE)")
        print(f"  필터 조회   {before['filter_seconds']:>5.2f}초 → "
              f"{after['filter_seconds']:>5.2f}초 → "
              f"{zorder['filter_seconds']:>5.2f}초 (Z-Order)")
        for codec in CODECS:
            row = results[f"codec_{codec}"]
            print(f"  {codec:6} {row['bytes'] / 1e6:>8.1f}MB  "
                  f"쓰기 {row['write_seconds']:>5.2f}초  "
                  f"읽기 {row['full_scan_seconds']:>5.2f}초")

        config.write_measurement("delta-experiment", {
            "shuffle_partitions": args.shuffle_partitions,
            "probe_artist": artist,
            "results": results,
        })
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
