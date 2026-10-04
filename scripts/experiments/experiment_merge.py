"""Delta의 `MERGE INTO`와 순수 Parquet의 파티션 재작성을 견준다.

명세 6.5의 마지막 줄이고, **이 프로젝트가 Delta를 고른 근거 그 자체**다.

    Delta vs 순수 Parquet SCD2 병합 | MERGE INTO vs 파티션 재작성 소요 시간

## 무엇이 다른가

순수 Parquet은 **행 단위 갱신이 없다.** 파일이 불변이기 때문이다. 아티스트 한
명의 이력을 닫으려면 그 행이 든 파일을 통째로 다시 써야 하고, 어느 파일에
들었는지 모르면 **테이블 전체를 다시 쓴다.**

Delta는 트랜잭션 로그로 "어느 파일이 지금 테이블인가"를 관리하므로, 바뀐 행이
든 파일만 다시 쓰고 나머지는 그대로 둔 채 로그만 고친다.

**그래서 비교의 핵심은 시간이 아니라 "얼마나 다시 썼는가"다.** 시간은 기계에
따라 달라지지만 쓴 바이트는 구조에서 나온다.

## 바꾸는 양을 늘려 가며 본다

한 명만 바꿀 때와 만 명을 바꿀 때 격차가 다르다. Parquet은 몇 명을 바꾸든
전체를 쓰므로 **거의 일정하고**, Delta는 바꾸는 양에 따라 는다. 어디서 만나는지가
"Delta가 언제 이득인가"의 답이다.

    ./scripts/spark_run.sh -- python scripts/experiments/experiment_merge.py
"""

import argparse
import shutil
import sys
import time
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from flux.common import config, deltalog
from flux.common.spark import session as spark_session
from flux.gold import scd2

CHANGE_SIZES = (1, 100, 10_000)
NEW_NAME = "병합 실험용 이름"


def folder_bytes(path) -> int:
    return sum(p.stat().st_size for p in Path(path).rglob("*.parquet"))


def timed(fn):
    started = time.time()
    value = fn()
    return value, round(time.time() - started, 2)


def targets(spark, dim_path: str, n: int) -> list:
    """바꿀 아티스트를 고른다. MBID 순으로 앞에서 n명.

    무작위로 고르면 실행마다 다른 파일에 흩어져 Delta 쪽 결과가 흔들린다.
    순서대로 고르면 몇 개 파일에 모여 있어 **재현된다.**
    """
    rows = (
        spark.read.format("delta").load(dim_path)
        .filter(F.col(scd2.IS_CURRENT))
        .select(scd2.BUSINESS_KEY).orderBy(scd2.BUSINESS_KEY)
        .limit(n).collect()
    )
    return [r[scd2.BUSINESS_KEY] for r in rows]


def run_delta(spark, source_path: str, work: Path, mbids: list, at: str) -> dict:
    """Delta로 병합한다. 바뀐 행이 든 파일만 다시 쓴다."""
    target = str(work / "delta")
    shutil.rmtree(target, ignore_errors=True)
    spark.read.format("delta").load(source_path) \
         .write.format("delta").mode("overwrite").save(target)

    before_files = set(deltalog.active_files(target))
    before_bytes = folder_bytes(target)

    changes = spark.createDataFrame([(m,) for m in mbids], [scd2.BUSINESS_KEY])
    table = DeltaTable.forPath(spark, target)
    _, took = timed(lambda: (
        table.alias("t")
        .merge(changes.alias("s"),
               f"t.{scd2.BUSINESS_KEY} = s.{scd2.BUSINESS_KEY} "
               f"AND t.{scd2.IS_CURRENT} = true")
        .whenMatchedUpdate(set={scd2.VALID_TO: F.lit(at),
                                scd2.IS_CURRENT: F.lit(False)})
        .execute()
    ))

    after_files = set(deltalog.active_files(target))
    rewritten = after_files - before_files
    return {
        "engine": "delta",
        "seconds": took,
        "files_before": len(before_files),
        "files_rewritten": len(rewritten),
        "bytes_written": sum(
            (Path(target) / f).stat().st_size for f in rewritten),
        "bytes_total": before_bytes,
    }


def run_parquet(spark, source_path: str, work: Path, mbids: list,
                at: str) -> dict:
    """순수 Parquet으로 같은 일을 한다. 행 단위 갱신이 없어 전체를 다시 쓴다."""
    target = str(work / "parquet")
    shutil.rmtree(target, ignore_errors=True)
    spark.read.format("delta").load(source_path) \
         .write.mode("overwrite").parquet(target)

    before_bytes = folder_bytes(target)
    wanted = F.array(*[F.lit(m) for m in mbids])

    def rewrite():
        # 읽고, 고치고, 통째로 다시 쓴다. 같은 경로에 바로 쓸 수 없어
        # 옆에 쓴 뒤 바꾼다. **이 교체가 원자적이지 않다는 것이 요점이다.**
        staged = str(work / "parquet_staged")
        shutil.rmtree(staged, ignore_errors=True)
        (
            spark.read.parquet(target)
            .withColumn(
                scd2.VALID_TO,
                F.when(F.array_contains(wanted, F.col(scd2.BUSINESS_KEY))
                       & F.col(scd2.IS_CURRENT), F.lit(at))
                 .otherwise(F.col(scd2.VALID_TO)))
            .withColumn(
                scd2.IS_CURRENT,
                F.when(F.array_contains(wanted, F.col(scd2.BUSINESS_KEY))
                       & F.col(scd2.IS_CURRENT), F.lit(False))
                 .otherwise(F.col(scd2.IS_CURRENT)))
            .write.mode("overwrite").parquet(staged)
        )
        shutil.rmtree(target, ignore_errors=True)
        Path(staged).rename(target)

    _, took = timed(rewrite)
    return {
        "engine": "parquet",
        "seconds": took,
        "files_before": None,
        "files_rewritten": len(list(Path(target).rglob("*.parquet"))),
        "bytes_written": folder_bytes(target),
        "bytes_total": before_bytes,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--at", default="2026-12-31")
    args = parser.parse_args()

    dim_path = config.gold(scd2.TABLE_NAME)
    work = config.local_root() / "experiments" / "merge"
    work.mkdir(parents=True, exist_ok=True)

    spark = spark_session("flux-merge-experiment",
                          shuffle_partitions=args.shuffle_partitions)
    try:
        rows = spark.read.format("delta").load(dim_path).count()
        print(f"차원 행 : {rows:,}")
        print()
        print(f"{'바꾼 수':>8} {'엔진':8} {'소요':>8} "
              f"{'다시 쓴 파일':>12} {'다시 쓴 바이트':>14} {'전체 대비':>9}")

        results = []
        for n in CHANGE_SIZES:
            mbids = targets(spark, dim_path, n)
            for runner in (run_delta, run_parquet):
                row = runner(spark, dim_path, work, mbids, args.at)
                row["changed"] = n
                results.append(row)
                share = (row["bytes_written"] / row["bytes_total"] * 100
                         if row["bytes_total"] else 0)
                print(f"{n:>8,} {row['engine']:8} {row['seconds']:>7.2f}초 "
                      f"{row['files_rewritten']:>12,} "
                      f"{row['bytes_written'] / 1e6:>13.1f}MB "
                      f"{share:>8.1f}%")

        print()
        print("정리")
        for n in CHANGE_SIZES:
            pair = {r["engine"]: r for r in results if r["changed"] == n}
            if len(pair) != 2:
                continue
            d, p = pair["delta"], pair["parquet"]
            faster = p["seconds"] / d["seconds"] if d["seconds"] else 0
            lighter = (p["bytes_written"] / d["bytes_written"]
                       if d["bytes_written"] else 0)
            print(f"  {n:>6,}명 변경  시간 {faster:>5.1f}배 빠름, "
                  f"쓴 바이트 {lighter:>6.1f}배 적음")

        config.write_measurement("merge-experiment", {
            "dim_rows": rows,
            "at": args.at,
            "shuffle_partitions": args.shuffle_partitions,
            "runs": results,
        })
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
