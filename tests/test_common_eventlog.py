"""Spark 이벤트 로그에서 실험 수치를 꺼내는 규칙.

픽스처는 실제 이벤트 로그의 모양을 그대로 쓴다. 상상한 형식으로 테스트하면
테스트만 통과하고 실물에서 깨진다.
"""

import json

import pytest

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


# ── 로그를 어디서 찾는가 ────────────────────────────────────

def rolling(tmp_path, parts, *, suffix=""):
    """Spark 4가 쓰는 롤링 로그 모양을 만든다.

    파일 하나가 아니라 디렉터리다. 이걸 몰라서 파일만 찾다가 아무것도
    못 찾았고, 그런데도 0을 돌려줘서 스큐가 없는 것처럼 보였다.
    """
    inner = tmp_path / "eventlog_v2_local-123"
    inner.mkdir(parents=True)
    (inner / "appstatus_local-123").write_text("", encoding="utf-8")
    for n, lines in parts.items():
        (inner / f"events_{n}_local-123{suffix}").write_text(
            "\n".join(lines), encoding="utf-8")
    return tmp_path


def test_디렉터리를_줘도_찾는다(tmp_path):
    t = rolling(tmp_path, {1: [task_end(1, 10)]})
    assert len(eventlog.event_files(t)) == 1


def test_표시_파일은_안_읽는다(tmp_path):
    """appstatus는 비어 있는 표시 파일이다. 읽으면 아무것도 안 나온다."""
    t = rolling(tmp_path, {1: [task_end(1, 10)]})
    assert all(p.name.startswith("events_") for p in eventlog.event_files(t))


def test_조각을_번호순으로_잇는다(tmp_path):
    """events_10_이 events_2_보다 뒤여야 한다. 사전순은 반대로 준다."""
    t = rolling(tmp_path, {1: [], 2: [], 10: []})
    order = [eventlog._order(p) for p in eventlog.event_files(t)]
    assert order == [1, 2, 10]


def test_여러_조각을_합쳐_읽는다(tmp_path):
    t = rolling(tmp_path, {1: [task_end(1, 10)], 2: [task_end(1, 20)]})
    assert eventlog.read(t)["stages"][0].tasks == 2


def test_파일_하나를_줘도_읽는다(tmp_path):
    flat = tmp_path / "plain"
    flat.write_text(task_end(1, 10), encoding="utf-8")
    assert eventlog.read(flat)["stages"][0].tasks == 1


def test_로그가_없으면_멈춘다(tmp_path):
    """조용히 0을 돌려주면 스큐가 없다는 측정값과 구분이 안 된다."""
    empty = tmp_path / "eventlog_v2_local-123"
    empty.mkdir(parents=True)
    (empty / "appstatus_local-123").write_text("", encoding="utf-8")
    with pytest.raises(eventlog.NoEventLog):
        eventlog.event_files(tmp_path)


def test_zstd로_눌린_것을_읽는다(tmp_path):
    """Spark 4는 이벤트 로그를 기본으로 압축한다."""
    from compression.zstd import compress

    inner = tmp_path / "eventlog_v2_local-123"
    inner.mkdir(parents=True)
    body = "\n".join([task_end(1, 10), task_end(1, 90)]).encode("utf-8")
    (inner / "events_1_local-123.zstd").write_bytes(compress(body))
    assert eventlog.read(tmp_path)["stages"][0].max_over_median == 1.8


# ── 큰 단계와 치우친 단계는 다르다 ──────────────────────────

def test_가장_무거운_단계와_가장_치우친_단계가_다를_수_있다():
    """실물에서 이랬다. 조인 잡의 가장 무거운 단계는 입력 파일을 읽는
    스캔(569 태스크, 비율 1.09)이고, 스큐는 셔플 뒤 조인 단계에
    있었다(200 태스크, 비율 4.94).

    총 시간으로 고르면 스캔이 20배 차이로 이긴다. heaviest를 스큐 지표로
    쓰면 '스큐가 없다'는 틀린 답이 나온다."""
    scan = [task_end(5, 4000) for _ in range(20)] + [task_end(5, 4400)]
    join = [task_end(6, 400) for _ in range(20)] + [task_end(6, 2000)]
    parsed = eventlog.parse(scan + join)

    assert eventlog.heaviest(parsed["stages"]).stage_id == 5


def _join_job():
    """조인 잡 하나의 모양. 실물(클러스터 aqe_on-1)의 단계 셋을 본떴다.

        Delta 로그   50 태스크, 셔플 읽기 21.7MB, 최장/중앙 12.00
        스캔         569 태스크, 셔플 읽기 0, 시간 합 최대
        조인         14 태스크, 셔플 읽기 855MB, 최장/중앙 1.24
    """
    delta_log = ([task_end(2, 270, read_remote=434_000) for _ in range(49)]
                 + [task_end(2, 3240, read_remote=434_000)])
    scan = [task_end(7, 2176, write=1_500_000) for _ in range(569)]
    join = ([task_end(10, 4020, read_remote=61_000_000) for _ in range(13)]
            + [task_end(10, 4968, read_remote=61_000_000)])
    return eventlog.parse(delta_log + scan + join)


def test_조인_단계는_셔플을_가장_많이_읽는_단계다():
    """비율이 가장 큰 단계를 고르면 Delta 로그 단계가 이긴다.

    실물에서 그렇게 틀렸다. 일이 거의 없는 장부 단계는 태스크 하나만 늦어도
    비율이 튄다. 그 12.00이 'AQE가 오히려 스큐를 키웠다'로 읽힐 뻔했다.
    로컬 aqe_on 세 런과 both 한 런의 기록도 이 단계의 값이었다.

    재려는 것은 조인의 셔플이다. 조인의 받는 쪽 단계는 양쪽의 셔플 출력을
    전부 읽으므로, 셔플 읽기가 가장 큰 단계가 곧 그 단계다. 문턱값이 필요 없다.
    """
    stages = _join_job()["stages"]
    assert eventlog.join_stage(stages).stage_id == 10


def test_조인_단계는_가장_무거운_단계와_다를_수_있다():
    stages = _join_job()["stages"]
    assert eventlog.heaviest(stages).stage_id == 7
    assert eventlog.join_stage(stages).stage_id == 10


def test_셔플이_없으면_조인_단계도_없다():
    parsed = eventlog.parse([task_end(1, 10), task_end(1, 20)])
    assert eventlog.join_stage(parsed["stages"]) is None


def test_요약이_둘을_따로_담는다():
    s = eventlog.summary(_join_job())
    assert s["heaviest"]["stage_id"] == 7
    assert s["join_stage"]["stage_id"] == 10
    assert "most_skewed" not in s
    assert len(s["all_stages"]) == 3


# ── 워커를 잃은 뒤 다시 한 일 ───────────────────────────────

def ended(stage_id, executor, *, reason="Success", attempt=0, stage_attempt=0):
    return json.dumps({
        "Event": "SparkListenerTaskEnd",
        "Stage ID": stage_id,
        "Stage Attempt ID": stage_attempt,
        "Task End Reason": {"Reason": reason},
        "Task Info": {"Executor ID": executor, "Attempt": attempt},
        "Task Metrics": {"Executor Run Time": 1},
    })


def removed(executor):
    return json.dumps({"Event": "SparkListenerExecutorRemoved",
                       "Executor ID": executor, "Removed Reason": "worker lost"})


def stage_done(stage_id, attempt):
    return json.dumps({"Event": "SparkListenerStageCompleted",
                       "Stage Info": {"Stage ID": stage_id,
                                      "Stage Attempt ID": attempt}})


def test_아무_일도_없었으면_재계산이_아니다():
    """이것을 막으려고 만든 함수다. 답이 같아도 통과로 치면 안 된다."""
    rec = eventlog.recovery([ended(0, "0"), ended(0, "1"), ended(1, "2")])
    assert rec["executors_removed"] == []
    assert not eventlog.recomputed(rec)


def test_실행기를_잃고_다시_하면_재계산이다():
    rec = eventlog.recovery([
        ended(0, "0"), ended(0, "1"), ended(0, "1"),
        removed("1"),
        ended(1, "0", reason="FetchFailed"),
        ended(0, "0", stage_attempt=1), ended(0, "2", stage_attempt=1),
        stage_done(0, 1),
        ended(1, "0", stage_attempt=1),
    ])
    assert rec["executors_removed"] == ["1"]
    assert rec["tasks_done_by_removed"] == 2
    assert rec["failed_tasks"] == {"FetchFailed": 1}
    assert rec["resubmitted_stages"] == [0, 1]
    assert eventlog.recomputed(rec)


def test_일이_없던_실행기를_잃은_것은_재계산이_아니다():
    """빈 워커를 죽이면 잃은 것이 없다. 장애를 견딘 게 아니라 장애가 없었다."""
    rec = eventlog.recovery([ended(0, "0"), ended(0, "0"), removed("2"),
                             ended(1, "0")])
    assert rec["executors_removed"] == ["2"]
    assert rec["tasks_done_by_removed"] == 0
    assert not eventlog.recomputed(rec)


def test_잃기만_하고_다시_한_것이_없으면_재계산이_아니다():
    """잡이 거의 끝난 뒤 죽였으면 다시 할 것이 없다."""
    rec = eventlog.recovery([ended(0, "1"), ended(1, "0"), removed("1")])
    assert rec["tasks_done_by_removed"] == 1
    assert not eventlog.recomputed(rec)


def test_재시도로_성공한_태스크를_센다():
    """실행 중이던 태스크가 죽으면 같은 단계 안에서 다시 시도된다."""
    rec = eventlog.recovery([
        ended(0, "1"), removed("1"),
        ended(0, "1", reason="ExecutorLostFailure"),
        ended(0, "0", attempt=1),
    ])
    assert rec["failed_tasks"] == {"ExecutorLostFailure": 1}
    assert rec["retried_tasks"] == 1
    assert eventlog.recomputed(rec)


def test_실패한_태스크는_끝낸_일로_세지_않는다():
    """실행기가 죽으면서 실패로 끝난 태스크는 남긴 출력이 없다."""
    rec = eventlog.recovery([
        ended(0, "1", reason="ExecutorLostFailure"), removed("1"),
    ])
    assert rec["tasks_done_by_removed"] == 0


def test_재계산_집계도_깨진_줄을_건너뛴다():
    """죽는 잡의 로그는 마지막 줄이 반쪽일 수 있다."""
    rec = eventlog.recovery([ended(0, "1"), removed("1"), '{"Event": "Spark'])
    assert rec["executors_removed"] == ["1"]


# ── 실행기 메모리 ───────────────────────────────────────────

def with_memory(heap, storage, execution=0, gc=0):
    return json.dumps({
        "Event": "SparkListenerTaskEnd", "Stage ID": 0, "Stage Attempt ID": 0,
        "Task End Reason": {"Reason": "Success"},
        "Task Info": {"Executor ID": "driver", "Attempt": 0},
        "Task Metrics": {"Executor Run Time": 1},
        "Task Executor Metrics": {"JVMHeapMemory": heap,
                                  "OnHeapStorageMemory": storage,
                                  "OnHeapExecutionMemory": execution,
                                  "TotalGCTime": gc},
    })


def test_지표마다_따로_최고값을_고른다():
    """힙이 가장 컸던 순간과 저장 메모리가 가장 컸던 순간은 다를 수 있다."""
    peak = eventlog.peak_memory([
        with_memory(900, 10, gc=5), with_memory(300, 200, execution=70, gc=40),
    ])
    assert peak == {"jvm_heap": 900, "storage": 200, "execution": 70, "gc_ms": 40}


def test_메모리_지표가_없는_태스크는_건너뛴다():
    """실패한 태스크나 오래된 로그에는 이 칸이 비어 있을 수 있다."""
    bare = task_end(0, 10)
    assert eventlog.peak_memory([bare, with_memory(5, 6)])["storage"] == 6
    assert eventlog.peak_memory([bare])["jvm_heap"] == 0
