import json
import os
import socket
import sys

from pyspark.sql import SparkSession

MASTER = os.environ.get("SPARK_MASTER_URL", "spark://spark-master:7077")
PARTITIONS = int(os.environ.get("CHECK_PARTITIONS", "12"))


def executor_hosts(spark, partitions):
    """각 파티션이 어느 실행 프로세스에서 처리됐는지 모은다."""
    rdd = spark.sparkContext.parallelize(range(partitions * 20), partitions)
    pairs = rdd.map(lambda _: (socket.gethostname(), 1)).reduceByKey(lambda a, b: a + b)
    return dict(pairs.collect())


def wordcount(spark):
    text = ["the quick brown fox", "jumps over the lazy dog", "the fox"]
    rdd = spark.sparkContext.parallelize(text, 3)
    counts = rdd.flatMap(str.split).map(lambda w: (w, 1)).reduceByKey(lambda a, b: a + b)
    return dict(counts.collect())


def main():
    spark = (
        SparkSession.builder.appName("flux-cluster-check")
        .master(MASTER)
        .config("spark.ui.enabled", "false")
        .config("spark.cores.max", "6")
        .config("spark.executor.memory", "1g")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )
    try:
        hosts = executor_hosts(spark, PARTITIONS)
        counts = wordcount(spark)
        result = {
            "master": MASTER,
            "spark_version": spark.version,
            "executor_hosts": hosts,
            "distinct_hosts": len(hosts),
            "wordcount_the": counts.get("the"),
            "wordcount_fox": counts.get("fox"),
        }
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
