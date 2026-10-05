"""잡의 main을 감싸 끝날 때 실행 기록을 한 줄 남긴다. 기록의 모양은 flux.common.runs다.

    if __name__ == "__main__":
        sys.exit(job_runs.run(main))

잡 이름은 스크립트 파일 이름이다. 인자는 명령줄 그대로, 결과는 그 잡이 이번에 남긴
측정 기록의 이름이다.

**기록을 남기다 실패해도 잡의 결과를 바꾸지 않는다.** 기록은 부수적인 것이라, 기록 표에
쓰지 못했다고 성공한 잡이 실패로 끝나면 안 되고, 실패한 잡의 예외가 기록 쪽 예외에
가려져도 안 된다.

잡들은 끝날 때 자기 Spark 세션을 끈다. 그래서 기록은 작은 세션을 새로 띄워 쓴다. 이미
켜진 세션이 있으면(테스트) 그것을 쓰고 끄지 않는다.
"""

import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path

from flux.common import config, runs


def _append(row: dict) -> None:
    from pyspark.sql import SparkSession

    from flux.common.spark import session

    active = SparkSession.getActiveSession()
    spark = active or session("flux-job-runs", shuffle_partitions=1, memory="1g")
    try:
        (spark.createDataFrame([row], runs.SCHEMA)
         .write.format("delta").mode("append").partitionBy("run_date")
         .save(config.ops(runs.TABLE_NAME)))
    finally:
        if active is None:
            spark.stop()


def run(main, *, job: str | None = None, append=_append) -> int:
    name = job or Path(sys.argv[0]).stem
    started = datetime.now(timezone.utc)
    before = len(config.written_measurements())
    exit_code = None
    error = None
    try:
        exit_code = main()
        return exit_code
    except BaseException as exc:
        error = "".join(traceback.format_exception_only(type(exc), exc)).strip()
        raise
    finally:
        row = runs.record(
            name,
            run_id=str(uuid.uuid4()),
            started_at=started,
            finished_at=datetime.now(timezone.utc),
            exit_code=exit_code,
            arguments=sys.argv[1:],
            measurements=config.written_measurements()[before:],
            error=error,
        )
        try:
            append(row)
            print(f"실행 기록: {name} {row['status']} {row['seconds']}초")
        except Exception as exc:
            print(f"실행 기록을 남기지 못했다: {type(exc).__name__}: {exc}", file=sys.stderr)
