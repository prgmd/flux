"""Spark 이벤트 로그에서 실험 수치를 꺼내는 규칙.

픽스처는 실제 이벤트 로그의 모양을 그대로 쓴다. 상상한 형식으로 테스트하면
테스트만 통과하고 실물에서 깨진다.
"""

import json

from flux.common import eventlog


def task_end(stage_id, run_ms, *, read_remote=0, read_local=0, write=0):
    return json.dumps({
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": 0,
        "Task Type": "ShuffleMapTask",
        "Task End Reason": {"Reason": "Success"},
        "Task Info": {"Task ID": 0, "Launch Time": 0, "Finish Time": run_ms},
        "Task Metrics": {
            "Executor Run Time": run_ms,
            "Shuffle Read Metrics": {
                "Remote Bytes Read": read_remote,
                "Local Bytes Read": read_local,
            },
            "Shuffle Write Metrics": {"Shuffle Bytes Written": write},
        },
    })


# ── 읽어 들이기 ─────────────────────────────────────────────

def test_태스크를_단계별로_묶는다():
    parsed = eventlog.parse([
        task_end(1, 10), task_end(1, 20), task_end(2, 30),
    ])
    assert [s.stage_id for s in parsed["stages"]] == [1, 2]
    assert parsed["stages"][0].tasks == 2


def test_단계를_번호순으로_돌려준다():
    """로그에 나온 순서가 아니라 번호순이어야 표가 매번 같은 모양이 된다."""
    parsed = eventlog.parse([task_end(5, 1), task_end(2, 1), task_end(9, 1)])
    assert [s.stage_id for s in parsed["stages"]] == [2, 5, 9]


def test_태스크가_아닌_이벤트는_무시한다():
    """이벤트 로그의 대부분은 우리가 안 보는 것들이다."""
    other = json.dumps({"Event": "SparkListenerLogStart", "Spark Version": "4.2.0"})
    parsed = eventlog.parse([other, task_end(1, 10)])
    assert len(parsed["stages"]) == 1


def test_깨진_줄을_건너뛴다():
    """잡이 죽는 중에 끊기면 마지막 줄이 반쪽이다. 그것 때문에 앞의 멀쩡한
    수치를 다 버릴 이유가 없다."""
    parsed = eventlog.parse([task_end(1, 10), '{"Event": "Spark', task_end(1, 20)])
    assert parsed["stages"][0].tasks == 2


def test_빈_줄을_건너뛴다():
    parsed = eventlog.parse(["", "  ", task_end(1, 10)])
    assert len(parsed["stages"]) == 1


def test_필드가_없어도_죽지_않는다():
    """이벤트 로그의 필드 이름은 Spark 버전에 따라 늘거나 빠진다."""
    bare = json.dumps({"Event": "SparkListenerTaskEnd", "Stage ID": 1})
    parsed = eventlog.parse([bare])
    assert parsed["stages"][0].durations == [0]


def test_단계_번호가_없으면_버린다():
    parsed = eventlog.parse([json.dumps({"Event": "SparkListenerTaskEnd"})])
    assert parsed["stages"] == []


# ── 스큐를 재는 값 ──────────────────────────────────────────

def test_최장과_중앙값의_비율을_낸다():
    """전체 소요만으로는 일이 많아서 느린 것과 한 태스크에 몰려서 느린 것이
    구분되지 않는다. 이 비율이 그것을 가른다."""
    parsed = eventlog.parse([task_end(1, d) for d in (10, 10, 10, 100)])
    assert parsed["stages"][0].max_over_median == 10.0


def test_고르면_비율이_1이다():
    parsed = eventlog.parse([task_end(1, 10) for _ in range(4)])
    assert parsed["stages"][0].max_over_median == 1.0


def test_태스크가_하나면_비율이_1이다():
    parsed = eventlog.parse([task_end(1, 42)])
    assert parsed["stages"][0].max_over_median == 1.0


def test_전부_0이면_나누지_않는다():
    """0으로 나누면 터진다. 아주 빠른 단계에서 실제로 나온다."""
    parsed = eventlog.parse([task_end(1, 0), task_end(1, 0)])
    assert parsed["stages"][0].max_over_median == 0.0


# ── 셔플 ────────────────────────────────────────────────────

def test_원격과_로컬_읽기를_합친다():
    """둘을 따로 두면 로컬 모드에서 읽기가 0으로 보인다. 로컬 모드는
    거의 전부 로컬 읽기다."""
    parsed = eventlog.parse([task_end(1, 1, read_remote=100, read_local=50)])
    assert parsed["stages"][0].shuffle_read == 150


def test_단계_안에서_셔플을_더한다():
    parsed = eventlog.parse([
        task_end(1, 1, write=100), task_end(1, 1, write=200),
    ])
    assert parsed["stages"][0].shuffle_write == 300


# ── 요약 ────────────────────────────────────────────────────

def test_가장_무거운_단계를_고른다():
    """단계가 수십 개라 전부 적으면 표가 안 읽힌다. 전체 소요를 정하는 것은
    가장 무거운 단계다."""
    parsed = eventlog.parse([
        task_end(1, 10), task_end(2, 500), task_end(3, 20),
    ])
    assert eventlog.heaviest(parsed["stages"]).stage_id == 2


def test_단계가_없으면_대표도_없다():
    assert eventlog.heaviest([]) is None


def test_요약이_게이트_항목을_전부_담는다():
    parsed = eventlog.parse([
        task_end(1, 10, write=100), task_end(1, 90, read_local=50),
    ])
    s = eventlog.summary(parsed)
    for key in ("total_task_ms", "shuffle_read_bytes", "shuffle_write_bytes",
                "heaviest"):
        assert key in s
    assert s["total_task_ms"] == 100
    assert s["heaviest"]["max_over_median"] == 1.8


def test_앱_시작과_끝으로_전체_시간을_낸다():
    parsed = eventlog.parse([
        json.dumps({"Event": "SparkListenerApplicationStart", "Timestamp": 1000}),
        task_end(1, 10),
        json.dumps({"Event": "SparkListenerApplicationEnd", "Timestamp": 4000}),
    ])
    assert parsed["app_ms"] == 3000


def test_끝나지_않았으면_전체_시간이_없다():
    """강제 종료된 잡이 그렇다. 0으로 두면 빨랐던 것처럼 읽힌다."""
    parsed = eventlog.parse([
        json.dumps({"Event": "SparkListenerApplicationStart", "Timestamp": 1000}),
        task_end(1, 10),
    ])
    assert parsed["app_ms"] is None
