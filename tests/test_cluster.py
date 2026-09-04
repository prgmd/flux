"""클러스터 통합 검증.

docker compose로 띄운 Spark 스탠드얼론 클러스터가 필요하다.
순수 로직 테스트와 분리해 `cluster` 마커를 붙였고, 기본 실행에서는 제외된다.

    docker compose up -d
    pytest -m cluster
"""

import json
import shutil
import subprocess

import pytest

pytestmark = pytest.mark.cluster

TIMEOUT = 600


def _run_check():
    if shutil.which("docker") is None:
        pytest.skip("docker를 찾을 수 없다")

    proc = subprocess.run(
        ["docker", "compose", "run", "--rm", "spark-client"],
        capture_output=True,
        text=True,
        timeout=TIMEOUT,
    )
    if proc.returncode != 0:
        pytest.skip(f"클러스터가 준비되지 않았다 (exit {proc.returncode})")

    # Spark 진행 표시줄이 캐리지 리턴으로 같은 줄에 섞여 나오므로 마지막 JSON만 뽑는다.
    for chunk in reversed(proc.stdout.replace("\r", "\n").split("\n")):
        chunk = chunk.strip()
        if chunk.startswith("{") and chunk.endswith("}"):
            return json.loads(chunk)
    pytest.fail(f"결과 JSON을 찾지 못했다:\n{proc.stdout[-2000:]}")


@pytest.fixture(scope="module")
def result():
    return _run_check()


def test_클러스터에_붙는다(result):
    assert result["master"] == "spark://spark-master:7077"
    assert result["spark_version"].startswith("4.2")


def test_태스크가_여러_워커에_분산된다(result):
    assert result["distinct_hosts"] >= 2, (
        f"한 워커에만 몰렸다: {result['executor_hosts']}. "
        "클러스터 구성이 잘못됐거나 워커가 하나만 살아 있다."
    )


def test_모든_파티션이_처리된다(result):
    assert sum(result["executor_hosts"].values()) == 12 * 20


def test_워드카운트_결과가_맞다(result):
    assert result["wordcount_the"] == 3
    assert result["wordcount_fox"] == 2
