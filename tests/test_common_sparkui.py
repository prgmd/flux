"""워커를 언제 죽일지 정하는 규칙.

픽스처는 Spark UI REST API의 `/stages` 응답 모양을 따른다.
"""

from flux.common import sparkui


MB = 1_000_000


def stage(stage_id, status, write=0):
    return {"stageId": stage_id, "status": status, "shuffleWriteBytes": write}


def test_셔플을_남기고_끝난_단계가_있고_다음이_돌면_죽인다():
    assert sparkui.ready_to_kill([stage(0, "COMPLETE", 800 * MB), stage(1, "ACTIVE")])


def test_첫_단계가_도는_중이면_아직이다():
    """지금 죽이면 실행 중이던 태스크만 다시 돈다. 잃은 출력이 없다."""
    assert not sparkui.ready_to_kill([stage(0, "ACTIVE"), stage(1, "PENDING")])


def test_셔플_없이_끝난_단계는_잃을_것이_아니다():
    """결과를 드라이버로 바로 돌려준 단계는 실행기 디스크에 남긴 게 없다."""
    assert not sparkui.ready_to_kill([stage(0, "COMPLETE", 0), stage(1, "ACTIVE")])


def test_전부_끝났으면_늦었다():
    assert not sparkui.ready_to_kill([stage(0, "COMPLETE", 800 * MB),
                                      stage(1, "COMPLETE", 0)])


def test_건너뛴_단계는_끝난_것으로_치지_않는다():
    """SKIPPED는 앞선 잡이 이미 만든 출력을 재사용한 것이다. 이 잡이 쓴 게 아니다."""
    assert not sparkui.ready_to_kill([stage(0, "SKIPPED", 800 * MB), stage(1, "ACTIVE")])


def test_어느_단계를_근거로_삼았는지_돌려준다():
    stages = [stage(2, "COMPLETE", 500 * MB), stage(0, "COMPLETE", 700 * MB),
              stage(3, "ACTIVE"), stage(1, "COMPLETE", 0)]
    assert sparkui.finished_shuffle_maps(stages) == [0, 2]
    assert sparkui.running(stages) == [3]


def test_Delta_장부_단계의_셔플로는_죽이지_않는다():
    """Delta는 테이블을 읽기 전에 트랜잭션 로그를 셔플해 파일 목록을 만든다.

    실물에서 그 셔플이 21.7MB였다. 하한이 없으면 본 작업이 시작하기도 전에
    죽이고, 장부만 다시 만든 것을 재계산이라고 부르게 된다. 스큐 지표가
    같은 장부 단계에 속았던 것과 같은 함정이다."""
    delta_log = [stage(0, "COMPLETE", 21 * MB), stage(1, "ACTIVE")]
    assert not sparkui.ready_to_kill(delta_log)


def test_본_작업의_첫_셔플이_끝나면_죽인다():
    stages = [stage(0, "COMPLETE", 21 * MB), stage(1, "COMPLETE"),
              stage(2, "COMPLETE", 900 * MB), stage(3, "ACTIVE")]
    assert sparkui.ready_to_kill(stages)
    assert sparkui.finished_shuffle_maps(stages) == [2]
