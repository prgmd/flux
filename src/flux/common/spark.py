"""Delta를 쓰는 Spark 세션을 한곳에서 만든다.

같은 설정 열두 줄이 열두 스크립트에 복사돼 있었다. 복사된 설정은 갈라진다.
한쪽만 고치면 그 스크립트만 다르게 동작하고, 다른 결과가 나와도
설정 차이인지 로직 차이인지 알 수 없다.

pyspark를 꼭대기에서 부르지 않는다. 호스트에는 Spark가 없고,
이 모듈을 import하는 것만으로 죽으면 곤란하다.
"""

import os

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

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master_url or master())
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", memory)
    )
    for key, value in (extra or {}).items():
        builder = builder.config(key, value)
    return configure_spark_with_delta_pip(builder).getOrCreate()
