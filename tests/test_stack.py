"""compose 스택 통합 검증.

docker compose로 띄운 PostgreSQL 2대와 Airflow가 필요하다.

    docker compose up -d
    pytest -m cluster
"""

import json
import shutil
import subprocess
import urllib.error
import urllib.request

import pytest

pytestmark = pytest.mark.cluster

AIRFLOW_API = "http://localhost:8081/api/v2/version"
TIMEOUT = 30


def _docker(*args):
    if shutil.which("docker") is None:
        pytest.skip("docker를 찾을 수 없다")
    proc = subprocess.run(
        ["docker", *args], capture_output=True, text=True, timeout=TIMEOUT
    )
    if proc.returncode != 0:
        pytest.skip(f"컨테이너가 준비되지 않았다: {' '.join(args)}")
    return proc.stdout.strip()


def _airflow_config(section, key):
    out = _docker(
        "exec", "flux-airflow-scheduler",
        "airflow", "config", "get-value", section, key,
    )
    # 경고 줄이 섞여 나오므로 마지막 비어 있지 않은 줄만 취한다.
    return [l for l in out.splitlines() if l.strip()][-1].strip()


@pytest.fixture(scope="module")
def version_payload():
    try:
        with urllib.request.urlopen(AIRFLOW_API, timeout=TIMEOUT) as resp:
            return json.load(resp)
    except (urllib.error.URLError, OSError) as exc:
        pytest.skip(f"Airflow API에 접속할 수 없다: {exc}")


def test_airflow_API가_응답한다(version_payload):
    assert version_payload["version"].startswith("3.1")


def test_executor가_LocalExecutor다():
    assert _airflow_config("core", "executor") == "LocalExecutor"


def test_메타DB가_SQLite가_아니다():
    conn = _airflow_config("database", "sql_alchemy_conn")
    assert conn.startswith("postgresql"), (
        f"메타DB가 PostgreSQL이 아니다: {conn}. "
        "SQLite는 SequentialExecutor만 지원해 태스크가 직렬화되고 실측이 왜곡된다."
    )


def test_예제_DAG를_불러오지_않는다():
    assert _airflow_config("core", "load_examples").lower() == "false"


def test_서빙DB가_응답한다():
    out = _docker(
        "exec", "flux-postgres-serving",
        "psql", "-U", "flux", "-d", "flux_serving", "-tAc", "SELECT 1",
    )
    assert out == "1"


def test_서빙DB와_메타DB가_분리되어_있다():
    serving = _docker(
        "exec", "flux-postgres-serving",
        "psql", "-U", "flux", "-d", "flux_serving", "-tAc",
        "SELECT current_database()",
    )
    meta = _docker(
        "exec", "flux-postgres-airflow",
        "psql", "-U", "airflow", "-d", "airflow", "-tAc",
        "SELECT current_database()",
    )
    assert serving == "flux_serving"
    assert meta == "airflow"
    assert serving != meta
