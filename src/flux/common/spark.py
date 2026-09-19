"""Delta를 쓰는 Spark 세션을 한곳에서 만든다.

같은 설정 열두 줄이 열두 스크립트에 복사돼 있었다. 복사된 설정은 갈라진다.
한쪽만 고치면 그 스크립트만 다르게 동작하고, 다른 결과가 나와도
설정 차이인지 로직 차이인지 알 수 없다.

pyspark를 꼭대기에서 부르지 않는다. 호스트에는 Spark가 없고,
이 모듈을 import하는 것만으로 죽으면 곤란하다.
"""

import os
import socket

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"

MASTER_ENV = "FLUX_SPARK_MASTER"
DEFAULT_MASTER = "local[*]"


def master() -> str:
    """어디에 붙을 것인가. 기본은 로컬 모드다.

    잡 전부가 로컬 모드로 돌아 왔다. compose의 standalone 클러스터에 붙는 것은
    scripts/cluster_check.py 하나뿐이었고, 그래서 **클러스터로 한 일이 없다.**

    S4의 스큐 실험에 직접 걸린다. 로컬 모드에서도 파티션 스큐는 보이지만
    셔플이 네트워크를 타지 않으므로 salting의 효과가 과소 측정된다.

    환경 변수로 빼 두면 코드를 고치지 않고 양쪽에서 재고 비교할 수 있다.

        FLUX_SPARK_MASTER=spark://spark-master:7077

    기본값을 바꾸지 않는다. 지금까지의 측정이 로컬 모드 결과이고,
    기본값을 옮기면 그 결과를 재현할 수 없다.
    """
    return os.environ.get(MASTER_ENV) or DEFAULT_MASTER


def _driver_address() -> dict:
    """클러스터에 붙을 때 워커가 드라이버를 찾아오게 하는 설정.

    **로컬 모드에는 없는 문제다.** 드라이버와 실행기가 같은 프로세스라
    서로 찾을 일이 없다.

    standalone에서는 워커가 드라이버에 되돌아 붙어야 결과와 셔플 정보를
    주고받는다. 그런데 컨테이너 안에서 호스트 이름을 물으면 컨테이너 자기
    이름이 나오고, 그것을 워커가 못 푸는 경우가 있다. 그래서 **IP를 직접**
    알려준다.

    bindAddress는 0.0.0.0으로 둔다. 드라이버가 어느 인터페이스로 들어오는
    연결이든 받게 하는 것이고, 컨테이너 네트워크에서는 이게 맞다.
    """
    return {
        "spark.driver.host": socket.gethostbyname(socket.gethostname()),
        "spark.driver.bindAddress": "0.0.0.0",
    }


def session(app_name: str, *, shuffle_partitions: int = 200, memory: str = "8g",
            master_url: str | None = None, extra: dict | None = None):
    """Delta 확장이 붙은 세션.

    잡마다 다른 것은 이름, 셔플 파티션 수, 드라이버 메모리 셋뿐이다.
    나머지는 전부 같으므로 여기서만 정한다.

    `master_url`은 테스트가 작은 세션을 만들 때 쓴다. 안 주면 master()가 정한다.

    `extra`는 S4의 스큐 실험이 쓴다. AQE를 켜고 끄거나 이벤트 로그를 남기는
    설정을 밖에서 주입한다. **기본값은 비어 있다.** 실험용 설정이 평소 잡에
    스며들면 지금까지의 측정과 비교할 수 없게 된다.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    target = master_url or master()
    builder = (
        SparkSession.builder.appName(app_name)
        .master(target)
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", memory)
    )
    # 클러스터일 때만 드라이버 주소를 잡는다. 로컬 모드에 넣으면 불필요하고,
    # 기존 측정과 설정이 달라져 재현이 어긋난다.
    settings = {} if target.startswith("local") else _driver_address()
    settings.update(extra or {})
    for key, value in settings.items():
        builder = builder.config(key, value)
    return configure_spark_with_delta_pip(builder).getOrCreate()
