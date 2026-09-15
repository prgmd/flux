"""Delta를 쓰는 Spark 세션을 한곳에서 만든다.

같은 설정 열두 줄이 열두 스크립트에 복사돼 있었다. 복사된 설정은 갈라진다.
한쪽만 고치면 그 스크립트만 다르게 동작하고, 다른 결과가 나와도
설정 차이인지 로직 차이인지 알 수 없다.

pyspark를 꼭대기에서 부르지 않는다. 호스트에는 Spark가 없고,
이 모듈을 import하는 것만으로 죽으면 곤란하다.
"""

DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def session(app_name: str, *, shuffle_partitions: int = 200, memory: str = "8g"):
    """Delta 확장이 붙은 로컬 모드 세션.

    잡마다 다른 것은 이름, 셔플 파티션 수, 드라이버 메모리 셋뿐이다.
    나머지는 전부 같으므로 여기서만 정한다.
    """
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", DELTA_EXTENSION)
        .config("spark.sql.catalog.spark_catalog", DELTA_CATALOG)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", memory)
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
