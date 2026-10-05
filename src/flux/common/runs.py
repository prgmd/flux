"""잡 실행 기록 한 줄의 모양. pyspark를 부르지 않는다.

잡이 끝나면 성공이든 실패든 한 줄을 남긴다. 언제 돌았고, 얼마나 걸렸고, 어떻게
끝났는지다. 지금까지는 그것이 터미널 출력과 측정 파일(`measurements/*.json`)에만
있었다. 측정 파일은 잡 이름마다 하나라 다시 돌리면 덮어써지고, 실패한 실행은 아예
남지 않는다.

Spark 잡만 남긴다. 덤프 수집과 압축 해제는 Spark를 쓰지 않는 파일 작업이라 Delta 표에
쓸 수단이 없고, 자동화 뒤에는 Airflow가 그 실행 이력을 남긴다.
"""

import json
from datetime import datetime

TABLE_NAME = "job_runs"

SUCCEEDED = "succeeded"
FAILED = "failed"

SCHEMA = (
    "run_id string, job string, status string, started_at timestamp, "
    "finished_at timestamp, seconds double, exit_code int, arguments string, "
    "measurements string, error string, run_date date"
)

MAX_ERROR = 500


def status_of(exit_code, error) -> str:
    """종료 코드가 0이고 예외가 없을 때만 성공이다.

    잡들은 확인에 실패하면 예외 대신 1을 돌려준다(대조 실패, 불변식 위반). 예외만 보면
    그런 실패가 성공으로 남는다.
    """
    return SUCCEEDED if exit_code == 0 and error is None else FAILED


def record(job: str, *, run_id: str, started_at: datetime, finished_at: datetime,
           exit_code, arguments, measurements, error) -> dict:
    return {
        "run_id": run_id,
        "job": job,
        "status": status_of(exit_code, error),
        "started_at": started_at,
        "finished_at": finished_at,
        "seconds": round((finished_at - started_at).total_seconds(), 1),
        "exit_code": exit_code,
        "arguments": json.dumps(list(arguments), ensure_ascii=False),
        "measurements": json.dumps(list(measurements), ensure_ascii=False),
        "error": None if error is None else error[:MAX_ERROR],
        "run_date": started_at.date(),
    }
