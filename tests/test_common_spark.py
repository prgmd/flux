"""Spark 세션을 만드는 규칙. pyspark 없이 확인할 수 있는 부분만 본다.

세션 자체는 컨테이너에서만 뜨므로, 여기서는 **어떤 설정을 넘기기로 정했는가**를
고정한다.
"""

from flux.common import spark


# ── 클러스터에 붙을 때의 드라이버 주소 ──────────────────────

def test_드라이버_주소가_IP다():
    """컨테이너 이름을 주면 워커가 못 푸는 경우가 있다. IP를 직접 준다."""
    import ipaddress
    conf = spark._driver_address()
    ipaddress.ip_address(conf["spark.driver.host"])


def test_바인드는_모든_인터페이스다():
    """컨테이너 네트워크에서는 어느 인터페이스로 들어오든 받아야 한다."""
    assert spark._driver_address()["spark.driver.bindAddress"] == "0.0.0.0"
