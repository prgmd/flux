"""적재된 Bronze의 규모와 디스크 사용을 잰다.

디렉터리 크기만 보면 안 된다. Delta는 덮어쓸 때 옛 파일을 지우지 않으므로
디스크에 있는 것과 지금 읽히는 것이 다르다. 그 차이가 곧 시점 조회의 대가이고,
VACUUM 보존 기간을 정하는 근거다.

    docker run ... -e FLUX_STORAGE_ROOT=file:///data flux-spark \
        python scripts/bronze_stats.py
"""

import argparse
import sys
from pathlib import Path

from delta import DeltaTable
from pyspark.sql import functions as F

from flux.common import config
from flux.common.spark import session as spark_session

BRONZE_TABLE = "raw_listens"
QUARANTINE_TABLE = "invalid_listens"


def on_disk(path: Path) -> tuple[int, int]:
    """(바이트, parquet 파일 수). 로그와 체크포인트까지 모두 센다."""
    if not path.exists():
        return 0, 0
    total = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    files = sum(1 for f in path.rglob("*.parquet"))
    return total, files


def gb(size: int) -> float:
    return round(size / 1024 ** 3, 2)


def main() -> int:
    parser = argparse.ArgumentParser()
    args = parser.parse_args()

    root = config.local_root()
    bronze_uri = config.bronze(BRONZE_TABLE)
    bronze_dir = root / "bronze" / BRONZE_TABLE
    spark = spark_session("flux-bronze-stats", memory="4g")
    try:
        # spark.sql의 `delta.\`경로\`` 문법은 카탈로그를 거치느라
        # 워크스테이션 경로 규칙에 걸린다. 경로로 직접 여는 API를 쓴다.
        table = DeltaTable.forPath(spark, bronze_uri)
        detail = table.detail().collect()[0]
        history = table.history().count()

        df = spark.read.format("delta").load(bronze_uri)
        rows = df.count()
        dumps = df.select("_source_dump").distinct().count()
        files = df.select("_source_file").distinct().count()
        span = df.agg(
            F.min("listened_at").alias("oldest"),
            F.max("listened_at").alias("newest"),
        ).collect()[0]

        quarantine = spark.read.format("delta").load(config.bronze(QUARANTINE_TABLE)).count()

        disk_bytes, disk_files = on_disk(bronze_dir)
        active_bytes = detail["sizeInBytes"]
        active_files = detail["numFiles"]
        archives, _ = on_disk(root / "dumps" / "listenbrainz" / "incremental")

        payload = {
            "rows": rows,
            "quarantine_rows": quarantine,
            "dumps": dumps,
            "source_files": files,
            "listened_at_min": span["oldest"],
            "listened_at_max": span["newest"],
            "delta_versions": history,
            "active_bytes": active_bytes,
            "active_files": active_files,
            "disk_bytes": disk_bytes,
            "disk_parquet_files": disk_files,
            "archive_bytes": archives,
        }

        print(f"덤프          : {dumps}개")
        print(f"원본 파일     : {files}개")
        print(f"행            : {rows:,}  (격리 {quarantine:,})")
        print(f"Delta 버전    : {history}개")
        print()
        print(f"원본 아카이브 : {gb(archives)}GB")
        print(f"활성 데이터   : {gb(active_bytes)}GB  ({active_files:,}개 파일)")
        print(f"디스크 점유   : {gb(disk_bytes)}GB  ({disk_files:,}개 파일)")
        print(f"회수 가능     : {gb(disk_bytes - active_bytes)}GB "
              f"({(disk_bytes - active_bytes) / disk_bytes * 100:.0f}%)")
        print()
        print(f"하루당 활성   : {gb(active_bytes / dumps)}GB")
        print(f"원본 대비     : {active_bytes / archives:.2f}배")

        config.write_measurement("bronze-stats", payload)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
