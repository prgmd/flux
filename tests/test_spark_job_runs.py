"""잡 실행 기록이 Delta 표에 실제로 쌓이는가."""

import pytest

pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from flux.common import config, job_runs, runs

pytestmark = pytest.mark.spark


def test_실행마다_한_줄씩_쌓인다(spark, monkeypatch, tmp_path):
    monkeypatch.setenv(config.STORAGE_ROOT_ENV, tmp_path.as_posix())
    job_runs.run(lambda: 0, job="first")
    job_runs.run(lambda: 1, job="second")

    rows = {r["job"]: r for r in
            spark.read.format("delta").load(config.ops(runs.TABLE_NAME)).collect()}
    assert rows["first"]["status"] == runs.SUCCEEDED
    assert rows["second"]["status"] == runs.FAILED
    assert rows["second"]["exit_code"] == 1
