"""컨테이너 실행 래퍼가 옛 코드를 조용히 돌리지 못하게 막는가.

이미지는 src와 scripts를 구워서 들고 있다. 마운트가 실패해도
ModuleNotFoundError가 나지 않고 **이미지 사본이 임포트된다.** 실제로 그렇게
됐다. Git Bash의 MSYS가 마운트 오른쪽의 `/opt/flux/src`를 윈도우 경로로
바꿔 버려 마운트가 엉뚱한 자리에 걸렸는데, 명령줄에는 마운트가 적혀 있으니
눈으로는 통과했다. 11일 전 코드로 실험이 돌 뻔했다.

래퍼가 그것을 막는 방식은 지문 대조다. 여기서 보는 것은 **대조가 의미를
갖는 조건**이다. 양쪽이 같은 계산을 써야 하고, 경로 변환이 꺼져 있어야 한다.
"""

from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "spark_run.sh"


@pytest.fixture(scope="module")
def source() -> str:
    if not SCRIPT.exists():
        pytest.fail(f"실행 래퍼가 없다: {SCRIPT}")
    return SCRIPT.read_text(encoding="utf-8")


def test_경로_변환을_끈다(source):
    """이것이 빠지면 마운트가 조용히 빗나간다. 사고의 직접 원인이다.

    주석이 아니라 실행되는 줄에 있어야 한다. 처음엔 문자열 포함만 봤는데
    설명 주석에도 같은 말이 있어서, 지워도 테스트가 통과했다."""
    lines = [line.strip() for line in source.splitlines()
             if not line.lstrip().startswith("#")]
    assert "export MSYS_NO_PATHCONV=1" in lines


def test_지문_계산이_양쪽에서_같은_식이다(source):
    """따로 적으면 갈라진다. 실제로 출력 형식 차이 하나로 멀쩡한 마운트가
    실패로 판정됐다. 한 곳에 두고 양쪽이 그것을 쓴다."""
    assert source.count("FINGERPRINT_CMD=") == 1
    body = source.split("FINGERPRINT_CMD=", 1)[1]
    assert body.count('"$FINGERPRINT_CMD"') == 2


def test_지문이_소스_전체를_덮는다(source):
    """한 파일만 보면 다른 파일이 옛것인 것을 놓친다.

    tests가 빠져 있었다. 이미지가 tests도 구워 들고 있으므로, 컨테이너에서
    도는 테스트가 옛 파일이어도 아무 말이 없었다. 새 테스트 세 건이 통과한
    줄 알았는데 실제로는 한 번도 안 돌았다."""
    command = source.split("FINGERPRINT_CMD=", 1)[1].split("\n", 1)[0]
    for part in ("src", "scripts", "tests", "pytest.ini", "*.py"):
        assert part in command, part


def test_지문이_덮는_것은_전부_마운트된다(source):
    """지문에 넣고 마운트를 빼면 매번 실패하고, 마운트하고 지문에서 빼면
    옛것을 못 잡는다. 둘이 같은 목록이어야 한다."""
    for part in ("src", "scripts", "tests", "pytest.ini"):
        assert f'"$repo/{part}:/opt/flux/{part}:ro"' in source, part


def test_다르면_잡을_돌리지_않는다(source):
    """경고만 내고 돌리면 막은 것이 아니다."""
    guard = source.split('if [ "$got" != "$FLUX_SRC_FINGERPRINT" ]', 1)[1]
    assert "exit 3" in guard.split("fi", 1)[0]


def test_데이터_루트가_없으면_멈춘다(source):
    """비어 있으면 컨테이너 안 빈 디렉터리를 보고 0행을 낸다.
    0행은 실패가 아니라 '데이터가 없다'로 읽혀서 더 위험하다."""
    assert "FLUX_DATA_DIR가 없다" in source
    assert "exit 2" in source
