import shutil
import sys
import tempfile
from pathlib import Path

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def build_session():
    builder = (
        SparkSession.builder.appName("flux-smoke-delta")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def check_roundtrip(spark, table_path):
    rows = [(1, "alpha"), (2, "beta"), (3, "gamma")]
    spark.createDataFrame(rows, "id INT, name STRING").write.format("delta").save(
        table_path
    )

    read_back = spark.read.format("delta").load(table_path)
    if read_back.count() != len(rows):
        raise AssertionError(
            f"왕복 실패: 쓴 행 {len(rows)}건, 읽은 행 {read_back.count()}건"
        )
    return read_back.count()


def check_merge(spark, table_path):
    updates = spark.createDataFrame([(2, "BETA"), (4, "delta")], "id INT, name STRING")
    target = DeltaTable.forPath(spark, table_path)

    (
        target.alias("t")
        .merge(updates.alias("s"), "t.id = s.id")
        .whenMatchedUpdate(set={"name": "s.name"})
        .whenNotMatchedInsertAll()
        .execute()
    )

    result = {
        row["id"]: row["name"]
        for row in spark.read.format("delta").load(table_path).collect()
    }
    expected = {1: "alpha", 2: "BETA", 3: "gamma", 4: "delta"}
    if result != expected:
        raise AssertionError(f"MERGE 실패: 기대 {expected}, 실제 {result}")
    return len(result)


def check_time_travel(spark, table_path):
    version_zero = spark.read.format("delta").option("versionAsOf", 0).load(table_path)
    if version_zero.count() != 3:
        raise AssertionError(
            f"시점 조회 실패: 버전 0의 행 수가 3이 아니라 {version_zero.count()}"
        )
    return version_zero.count()


def main():
    workdir = Path(tempfile.mkdtemp(prefix="flux-smoke-"))
    table_path = str(workdir / "smoke_table")
    spark = None
    try:
        spark = build_session()
        print(f"Python  : {sys.version.split()[0]}")
        print(f"Spark   : {spark.version}")

        print(f"왕복      : {check_roundtrip(spark, table_path)}행 OK")
        print(f"MERGE     : {check_merge(spark, table_path)}행 OK")
        print(f"시점 조회 : {check_time_travel(spark, table_path)}행 OK")
        print("\n스모크 테스트 통과")
        return 0
    except Exception as exc:
        print(f"\n스모크 테스트 실패: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1
    finally:
        if spark is not None:
            spark.stop()
        shutil.rmtree(workdir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
