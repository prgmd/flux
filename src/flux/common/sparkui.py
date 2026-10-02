"""Spark UI의 REST API가 돌려주는 단계 목록으로 판단한다.

워커 강제 종료 실험에서 **언제 죽일지**를 정하는 데 쓴다. 판단만 두고 HTTP는
부르지 않는다. 그래야 pyspark도 네트워크도 없이 호스트에서 시험할 수 있다.

단계 하나는 `/api/v1/applications/<id>/stages`의 원소 하나다. 쓰는 필드는 셋이다.

    status              ACTIVE, COMPLETE, PENDING, FAILED, SKIPPED
    shuffleWriteBytes   그 단계가 셔플로 내보낸 바이트
    stageId
"""

ACTIVE = "ACTIVE"
COMPLETE = "COMPLETE"


def finished_shuffle_maps(stages) -> list:
    """셔플 출력을 남기고 끝난 단계들.

    이 단계들의 출력은 그것을 쓴 실행기의 디스크에 있다. 이게 있어야 실행기를
    죽였을 때 **이미 끝난 일을 잃는다.**
    """
    return sorted(s["stageId"] for s in stages
                  if s.get("status") == COMPLETE
                  and s.get("shuffleWriteBytes", 0) > 0)


def running(stages) -> list:
    return sorted(s["stageId"] for s in stages if s.get("status") == ACTIVE)


def ready_to_kill(stages) -> bool:
    """지금 죽이면 계보로 다시 만드는 장면이 나오는가.

    둘 다 있어야 한다. 끝난 셔플 단계가 없으면 잃을 출력이 없고, 도는 단계가
    없으면 잡이 이미 끝났거나 아직 시작하지 않았다.
    """
    return bool(finished_shuffle_maps(stages)) and bool(running(stages))
