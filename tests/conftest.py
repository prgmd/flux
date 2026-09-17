"""번역 테스트가 쓰는 작은 Spark 세션.

이 저장소의 테스트는 두 종류다.

    판단 테스트   pyspark 없이, 호스트에서, 3초        (기본)
    번역 테스트   작은 로컬 Spark로, 이미지 안에서      (-m spark)

**판단은 틀린 적이 없다. 틀린 것은 번역이었다.**

    `_record_id` 하나를 식별자로 써서 부풀림 16배
    `explode`를 써서 9,119만 행이 조용히 사라짐
    2단계 MERGE가 강제 종료에 반쪽 상태를 남김

셋 다 순수 모듈이 아니라 그것을 Spark 표현으로 옮긴 쪽의 문제였고, 셋 다
**1.5억 행 잡을 돌려야** 보였다. 여기서 보는 것은 판단 모듈과 Spark 잡이
같은 규칙을 쓰는가다.

수십 행 픽스처로 돌지만 49개에 5분 28초가 걸린다. 병목은 JVM 기동(38초, 1회)이
아니라 Delta다 — 차원을 만들고 MERGE하는 테스트가 행 두세 개에도 각 13~22초다.
트랜잭션 하나의 고정 비용이고 데이터 크기와 무관하다.

**그래서 이 층이 판단 층을 대신하지 못한다.** 규칙은 판단 모듈에 두고 호스트에서
확인하는 쪽이 100배 빠르다. 여기서는 두 층이 갈라졌는지만 본다.

호스트에서는 건너뛴다. Spark 4.x는 JDK 17+를 요구하고 호스트에는 없다.
이미지에는 JDK 21이 들어 있다(docker/spark.Dockerfile).

    docker run --rm flux-spark python -m pytest -m spark
"""

import pytest


@pytest.fixture(scope="session")
def spark():
    """세션 하나를 테스트 전체가 나눠 쓴다.

    테스트마다 만들면 JVM 기동이 매번 붙어 번역 테스트가 판단 테스트만큼
    가볍다는 전제가 깨진다.

    pyspark가 없거나 JVM이 뜨지 않으면 건너뛴다. **실패가 아니라 건너뛰기다.**
    호스트에서 이 테스트가 붉게 나오면 진짜 실패와 구분할 수 없어진다.
    """
    pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
    pytest.importorskip("delta", reason="delta-spark가 없다")

    from flux.common.spark import session as make_session

    try:
        spark = make_session(
            "flux-tests",
            shuffle_partitions=2,
            memory="1g",
            master_url="local[2]",
        )
    except Exception as exc:  # noqa: BLE001 - JVM이 뜨지 않는 이유는 여러 가지다
        pytest.skip(f"Spark 세션을 만들지 못했다: {exc}")

    # 픽스처는 수십 행이다. 브로드캐스트 임계값이나 AQE가 끼어들면
    # 무엇을 재는지 흐려진다.
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    yield spark
    spark.stop()


@pytest.fixture
def warehouse(tmp_path):
    """테스트 하나가 쓰는 Delta 경로. 끝나면 tmp_path와 함께 사라진다.

    `file:///` 스킴을 붙여 돌려준다. Spark에 넘기는 경로는 스킴이 있어야 하고,
    Windows 드라이브 문자가 앞에 오면 슬래시 세 개가 필요하다.
    """
    def path(*names: str) -> str:
        target = tmp_path.joinpath(*names)
        raw = target.as_posix()
        return f"file:///{raw.lstrip('/')}"

    return path
