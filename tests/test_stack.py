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


AIRFLOW_CONTAINERS = [
    "flux-airflow-scheduler",
    "flux-airflow-apiserver",
    "flux-airflow-dag-processor",
    "flux-airflow-triggerer",
]


def test_모든_Airflow_컨테이너가_같은_JWT_서명키를_쓴다():
    """서명 키가 갈리면 태스크가 실행 API 서버 인증에 실패한다.

    실제로 겪은 실패다. 키를 지정하지 않으면 컨테이너마다 임의로 생성하고,
    태스크는 'Invalid auth token: Signature verification failed'로 죽는다.
    """
    secrets = {}
    for name in AIRFLOW_CONTAINERS:
        out = _docker("exec", name, "airflow", "config", "get-value", "api_auth", "jwt_secret")
        secrets[name] = [l for l in out.splitlines() if l.strip()][-1].strip()

    assert len(set(secrets.values())) == 1, f"서명 키가 컨테이너마다 다르다: {secrets}"


def test_DAG를_트리거하면_태스크가_실행된다():
    """스케줄러가 실제로 태스크를 실행하는지 끝에서 끝까지 확인한다."""
    import time

    run_id = f"pytest-{int(time.time())}"
    _docker("exec", "flux-airflow-scheduler",
            "airflow", "dags", "trigger", "flux_stack_smoke", "--run-id", run_id)

    deadline = time.time() + 180
    states = ""
    while time.time() < deadline:
        states = _docker("exec", "flux-airflow-scheduler",
                         "airflow", "tasks", "states-for-dag-run",
                         "flux_stack_smoke", run_id)
        if states.count("success") == 3 or "failed" in states:
            break
        time.sleep(5)

    assert states.count("success") == 3, f"세 태스크가 모두 성공하지 않았다: {states}"
