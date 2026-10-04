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

    ./scripts/spark_run.sh -- python scripts/experiments/experiment_delta.py
"""

import argparse
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from flux.common import config, deltalog
from flux.common.spark import session as spark_session
from flux.gold import mart

CODECS = ("snappy", "zstd")

# OPTIMIZE가 노리는 파일 크기. 기본값은 1GB인데 이 마트가 그보다 작아서
# **파일 하나로 합쳐졌고, 그러면 Z-Order가 잴 것이 없어진다.**
# 파일이 여럿 남도록 줄여 잡는다.
MAX_FILE_MB = 32


def active_bytes(path) -> int:
    """**살아 있는 파일만** 센다.

    폴더를 glob하면 안 된다. OPTIMIZE는 새 파일을 쓰고 옛 파일을 로그에서만
    지우므로 **디스크에는 둘 다 남는다.** 폴더 크기를 재면 최적화할수록
    "커지는" 결과가 나온다. 실제로 612MB가 1,297MB로, 다시 1,982MB로
    늘어나는 것을 보고서야 알아챘다.

    트랜잭션 로그가 어느 파일이 지금 테이블인지 알고 있다(flux.common.deltalog).
    """
    root = Path(path)
    return sum((root / name).stat().st_size
               for name in deltalog.active_files(root))


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
    size = active_bytes(path)
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

        # 2) 배치 비교. 같은 원본에서 사본 셋을 만들어 각각 다르게 둔다.
        #
        # **순서대로 걸면 안 된다.** 처음엔 한 사본에 OPTIMIZE를 걸고 그 위에
        # Z-Order를 걸었는데, OPTIMIZE가 파일을 1개로 합쳐 버려서 Z-Order가
        # 할 일이 없었다. data skipping은 **파일이 여럿일 때** 건너뛰는
        # 기법이라 하나면 건너뛸 대상이 없다.
        #
        # 그래서 사본을 따로 만들어 나란히 비교한다.
        print()
        print(f"배치 비교 (OPTIMIZE 목표 파일 크기 {MAX_FILE_MB}MB)")
        spark.conf.set("spark.databricks.delta.optimize.maxFileSize",
                       str(MAX_FILE_MB * 1024 * 1024))

        plans = [
            ("as_written", None, "쓴 그대로"),
            ("optimized", "OPTIMIZE delta.`{path}`", "OPTIMIZE"),
            ("zordered",
             "OPTIMIZE delta.`{path}` ZORDER BY (artist_mbid)", "Z-Order"),
        ]
        for key, sql, label in plans:
            target = str(root / "experiments" / f"mart_{key}")
            shutil.rmtree(target, ignore_errors=True)
            source.write.format("delta").mode("overwrite").save(target)
            took = 0.0
            if sql:
                _, took = timed(lambda: spark.sql(sql.format(path=target)))
            row = snapshot(spark, target, artist,
                           f"{label}{f' ({took}초)' if sql else ''}")
            row["plan"] = key
            row["rewrite_seconds"] = took
            results[key] = row

        print()
        print("정리")
        print(f"  {'배치':12} {'파일':>6} {'크기':>10} {'필터 조회':>10}")
        for key, _, label in plans:
            row = results[key]
            print(f"  {label:12} {row['active_files']:>6,} "
                  f"{row['bytes'] / 1e6:>9.1f}MB {row['filter_seconds']:>9.2f}초")
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
