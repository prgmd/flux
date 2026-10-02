"""Spark 이벤트 로그에서 실험이 요구하는 수치를 꺼낸다.

명세 S4 게이트가 이렇게 적고 있다.

    기록 항목이 빠짐없다: 태스크별 시간 분포, 최장 태스크와 중앙값의 비율,
    셔플 read/write 크기, 전체 소요 시간

**전체 소요 시간만으로는 스큐를 설명할 수 없다.** 느려진 이유가 일이 많아서인지
한 태스크에 몰려서인지 구분되지 않는다. 최장/중앙값 비율이 그것을 가른다.

이 수치를 어디서 얻는가가 문제였다. 잡들은 `spark.ui.enabled=false`로 도므로
UI의 REST API가 없다. 남는 길이 둘이다.

    리스너를 붙인다   py4j로 콜백을 걸어야 하고 잡 코드에 실험 장치가 섞인다
    **이벤트 로그**   Spark가 원래 남기는 것을 나중에 읽는다

뒤쪽을 골랐다. 잡은 설정 한 줄만 받고 평소처럼 돌면 되고, 파싱은 **pyspark 없이
호스트에서** 돌아 테스트가 가볍다.

이벤트 로그는 한 줄에 JSON 하나인 텍스트다. 우리가 보는 것은 태스크가 끝날 때
남는 `SparkListenerTaskEnd` 하나뿐이다.
"""

import json
from dataclasses import dataclass, field
from pathlib import Path

TASK_END = "SparkListenerTaskEnd"
APP_START = "SparkListenerApplicationStart"
APP_END = "SparkListenerApplicationEnd"


def _dig(source: dict, *names, default=0):
    """중첩된 키를 따라간다. 없으면 기본값.

    이벤트 로그의 필드 이름은 Spark 버전에 따라 늘거나 빠진다. 없는 것을
    만나면 죽는 대신 0으로 두고, 그 사실이 결과에 드러나게 한다.
    """
    current = source
    for name in names:
        if not isinstance(current, dict) or name not in current:
            return default
        current = current[name]
    return current if current is not None else default


def _median(values) -> float:
    rows = sorted(values)
    if not rows:
        return 0.0
    middle = len(rows) // 2
    return (float(rows[middle]) if len(rows) % 2
            else (rows[middle - 1] + rows[middle]) / 2)


@dataclass
class Stage:
    """단계 하나. 태스크 여럿이 같은 일을 나눠 한다.

    태스크마다 두 가지를 모은다. 걸린 시간(`durations`)과 셔플로 읽은 행 수
    (`records`)다. 둘을 따로 두는 이유는 `records_max_over_median` 참고.
    """

    stage_id: int
    durations: list = field(default_factory=list)
    records: list = field(default_factory=list)
    shuffle_read: int = 0
    shuffle_write: int = 0
    elapsed_ms: int = 0

    @property
    def tasks(self) -> int:
        return len(self.durations)

    @property
    def total_ms(self) -> int:
        return sum(self.durations)

    @property
    def max_ms(self) -> int:
        return max(self.durations) if self.durations else 0

    @property
    def median_ms(self) -> float:
        return _median(self.durations)

    @property
    def max_over_median(self) -> float:
        """태스크 시간의 최장/중앙. **스큐만 재는 값이 아니다.**

        세 가지가 섞인다. 한 키에 몰린 데이터(재려던 것), 첫 물결 태스크의 예열
        비용, 그리고 파티션 수. AQE가 파티션을 합치면 모든 태스크가 커져 중앙값이
        올라가고, 긴 태스크가 그대로여도 비율은 내려간다. 데이터 편중만 보려면
        `records_max_over_median`을 본다.
        """
        median = self.median_ms
        return self.max_ms / median if median else 0.0

    @property
    def records_max_over_median(self) -> float:
        """태스크가 읽은 행 수의 최대/중앙. **데이터가 한쪽에 몰린 정도.**

        시간과 달리 예열이나 GC에 흔들리지 않는다. 같은 입력이면 매번 같은 값이
        나온다. 조인 기준선에서 시간 비율은 4.94~5.65로 흔들렸지만 행 비율은
        세 번 다 같았다.
        """
        median = _median(self.records)
        return max(self.records) / median if median else 0.0

    def summary(self) -> dict:
        return {
            "stage_id": self.stage_id,
            "tasks": self.tasks,
            "max_ms": self.max_ms,
            "median_ms": self.median_ms,
            "max_over_median": round(self.max_over_median, 2),
            "records_max": max(self.records) if self.records else 0,
            "records_median": _median(self.records),
            "records_max_over_median": round(self.records_max_over_median, 2),
            "elapsed_ms": self.elapsed_ms,
            "total_task_ms": self.total_ms,
            "shuffle_read_bytes": self.shuffle_read,
            "shuffle_write_bytes": self.shuffle_write,
        }


def parse(lines) -> dict:
    """이벤트 로그 줄들을 읽어 단계별로 묶는다.

    깨진 줄은 건너뛴다. 로그가 잡이 죽는 중에 끊기면 마지막 줄이 반쪽일 수
    있는데, 그것 때문에 앞의 멀쩡한 수치를 다 버릴 이유가 없다.
    """
    stages: dict = {}
    app_start = app_end = None

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue

        kind = event.get("Event")
        if kind == APP_START:
            app_start = event.get("Timestamp")
            continue
        if kind == APP_END:
            app_end = event.get("Timestamp")
            continue
        if kind == STAGE_COMPLETED:
            info = event.get("Stage Info") or {}
            done = stages.get(info.get("Stage ID"))
            if done and info.get("Completion Time") and info.get("Submission Time"):
                done.elapsed_ms = info["Completion Time"] - info["Submission Time"]
            continue
        if kind != TASK_END:
            continue

        stage_id = event.get("Stage ID")
        if stage_id is None:
            continue
        stage = stages.setdefault(stage_id, Stage(stage_id=stage_id))

        metrics = event.get("Task Metrics") or {}
        # 태스크가 실제로 일한 시간. 대기 시간을 빼고 본다.
        stage.durations.append(_dig(metrics, "Executor Run Time"))
        stage.records.append(
            _dig(metrics, "Shuffle Read Metrics", "Total Records Read"))
        stage.shuffle_read += (
            _dig(metrics, "Shuffle Read Metrics", "Remote Bytes Read")
            + _dig(metrics, "Shuffle Read Metrics", "Local Bytes Read")
        )
        stage.shuffle_write += _dig(
            metrics, "Shuffle Write Metrics", "Shuffle Bytes Written")

    ordered = [stages[k] for k in sorted(stages)]
    return {
        "stages": ordered,
        "app_ms": (app_end - app_start) if app_start and app_end else None,
    }


# 롤링 이벤트 로그의 조각 이름. Spark 4는 로그를 **파일 하나가 아니라
# 디렉터리**로 쓴다.
#
#     eventlogs/<잡 이름>/eventlog_v2_<앱 id>/
#         appstatus_<앱 id>          비어 있는 표시 파일
#         events_1_<앱 id>.zstd      실제 내용
#
# 파일만 찾다가 아무것도 못 찾았고, **그런데도 0을 돌려줬다.** 그래서
# 최장/중앙 비율이 0.00으로 나왔는데 실험은 멀쩡히 끝난 것처럼 보였다.
EVENT_PREFIX = "events_"


class NoEventLog(Exception):
    """이벤트 로그를 못 찾았다.

    조용히 0을 돌려주면 **스큐가 없는 것처럼 보인다.** 실험 결과가 그대로
    표에 들어가고, 읽는 사람은 그것이 측정값인 줄 안다.
    """


def _order(path: Path):
    """events_2_...가 events_10_...보다 앞이어야 한다. 사전순은 반대로 준다."""
    parts = path.name.split("_")
    try:
        return int(parts[1])
    except (IndexError, ValueError):
        return 0


def event_files(path) -> list:
    """이벤트 파일들. 파일 하나를 줘도 되고 로그 디렉터리를 줘도 된다."""
    path = Path(path)
    if path.is_file():
        return [path]
    found = sorted((p for p in path.rglob(f"{EVENT_PREFIX}*") if p.is_file()),
                   key=_order)
    if not found:
        raise NoEventLog(f"이벤트 로그가 없다: {path}")
    return found


def _open(path: Path):
    """zstd로 눌린 것을 그대로 읽는다.

    Spark 4는 이벤트 로그를 기본으로 압축한다. 파이썬 3.14가 표준 라이브러리에
    zstd를 넣어(PEP 784) 따로 의존을 더하지 않아도 된다.
    """
    if path.suffix == ".zstd":
        from compression.zstd import ZstdFile
        import io as _io
        return _io.TextIOWrapper(ZstdFile(path, "rb"),
                                 encoding="utf-8", errors="replace")
    return open(path, encoding="utf-8", errors="replace")


def lines(path):
    """로그의 줄들. 조각이 여럿이면 순서대로 잇는다."""
    for part in event_files(path):
        with _open(part) as handle:
            yield from handle


def read(path) -> dict:
    """로그를 읽어 단계별로 묶는다."""
    return parse(lines(path))


# ── 워커를 잃은 뒤 무엇을 다시 했는가 ─────────────────────────

STAGE_COMPLETED = "SparkListenerStageCompleted"
EXECUTOR_REMOVED = "SparkListenerExecutorRemoved"
SUCCESS = "Success"


def recovery(events) -> dict:
    """워커를 잃은 잡에서 Spark가 다시 한 일을 센다.

    **답이 같다는 것만으로는 재계산이 일어났다는 증거가 못 된다.** 잡이 끝난
    뒤에 죽였거나, 죽인 워커가 맡은 일이 없었어도 답은 같다. 그러면 게이트가
    "재계산이 일어나고 결과가 정합한지"를 확인한 것이 아니라 아무 일도 없었던
    잡을 두 번 돌린 것이 된다.

    그래서 셋을 센다.

        사라진 실행기         그 실행기가 그때까지 끝낸 태스크 수와 함께
        실패한 태스크         사유별로. ExecutorLostFailure, FetchFailed 등
        다시 시도한 것        두 번째 이상 시도로 성공한 태스크, 시도 번호가
                              1 이상인 단계

    사라진 실행기가 끝낸 태스크가 있어야 **잃은 것이 있었다**고 말할 수 있다.
    셔플 출력은 그것을 쓴 실행기의 디스크에 있으므로, 실행기가 죽으면 이미
    끝난 태스크의 결과도 같이 사라진다.
    """
    removed = []
    done_by: dict = {}
    failed: dict = {}
    retried = 0
    resubmitted = set()

    for line in events:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue

        kind = event.get("Event")
        if kind == EXECUTOR_REMOVED:
            removed.append(str(event.get("Executor ID")))
        elif kind == STAGE_COMPLETED:
            info = event.get("Stage Info") or {}
            if info.get("Stage Attempt ID", 0) > 0:
                resubmitted.add(info.get("Stage ID"))
        elif kind == TASK_END:
            reason = (event.get("Task End Reason") or {}).get("Reason", "Unknown")
            info = event.get("Task Info") or {}
            if event.get("Stage Attempt ID", 0) > 0:
                resubmitted.add(event.get("Stage ID"))
            if reason != SUCCESS:
                failed[reason] = failed.get(reason, 0) + 1
                continue
            executor = str(info.get("Executor ID"))
            done_by[executor] = done_by.get(executor, 0) + 1
            if info.get("Attempt", 0) > 0:
                retried += 1

    return {
        "executors_removed": sorted(set(removed)),
        "tasks_done_by_removed": sum(done_by.get(e, 0) for e in set(removed)),
        "failed_tasks": dict(sorted(failed.items())),
        "retried_tasks": retried,
        "resubmitted_stages": sorted(s for s in resubmitted if s is not None),
    }


def recomputed(rec: dict) -> bool:
    """재계산이 일어났다고 말할 수 있는가.

    실행기가 사라졌고, 그 실행기가 끝낸 일이 있었고, Spark가 무언가를 다시
    했어야 한다. 셋 중 하나라도 없으면 장애를 견딘 것이 아니라 장애가 없었던 것이다.
    """
    redone = (sum(rec["failed_tasks"].values()) > 0
              or rec["retried_tasks"] > 0
              or bool(rec["resubmitted_stages"]))
    return bool(rec["executors_removed"]) and rec["tasks_done_by_removed"] > 0 and redone


def heaviest(stages):
    """가장 오래 걸린 단계. **전체 소요를 정하는 단계**다.

    스큐를 보는 자리가 아니다. 그렇게 썼다가 틀렸다 — 아래 join_stage 참고.
    """
    return max(stages, key=lambda s: s.total_ms, default=None)


def join_stage(stages):
    """조인의 받는 쪽 단계. **스큐를 보는 자리는 여기다.**

    두 번 틀린 끝에 이 정의가 됐다.

    처음엔 heaviest(총 태스크 시간이 가장 큰 단계)를 썼다. 조인 잡에서 그
    단계는 입력 파일을 읽는 스캔이고, 파일 단위로 나뉘어 원래 고르다
    (569 태스크, 최장/중앙 1.09). **큰 단계와 치우친 단계가 다르다.**

    그다음엔 "태스크 10개 이상인 단계 중 최장/중앙이 가장 큰 단계"를 썼다.
    이번엔 **Delta 트랜잭션 로그를 읽는 단계**가 이겼다. 50 태스크에 셔플 읽기
    21.7MB뿐인 장부 단계라, 태스크 하나만 늦어도 비율이 튄다. 클러스터 aqe_on에서
    12.00이 나와 'AQE가 스큐를 키웠다'로 읽힐 뻔했고, 거슬러 보니 로컬 aqe_on
    세 런과 both 한 런의 기록도 이 단계의 값이었다. **가장 치우친 단계가 재려는
    단계라는 보장이 없다.**

    재려는 것은 조인의 셔플이다. 조인의 받는 쪽은 양쪽 셔플 출력을 전부 읽으므로
    **셔플 읽기가 가장 큰 단계**가 곧 그 단계다. 문턱값도 없고, AQE가 파티션을
    합치거나 쪼개도 그 단계가 그대로 남는다.
    """
    shuffled = [s for s in stages if s.shuffle_read > 0]
    return max(shuffled, key=lambda s: s.shuffle_read, default=None)


def summary(parsed: dict) -> dict:
    """실험 한 번의 요약. 게이트가 요구한 항목을 전부 담는다."""
    stages = parsed["stages"]
    top = heaviest(stages)
    skewed = join_stage(stages)
    return {
        "app_ms": parsed["app_ms"],
        "stages": len(stages),
        "total_task_ms": sum(s.total_ms for s in stages),
        "shuffle_read_bytes": sum(s.shuffle_read for s in stages),
        "shuffle_write_bytes": sum(s.shuffle_write for s in stages),
        "heaviest": top.summary() if top else None,
        "join_stage": skewed.summary() if skewed else None,
        "all_stages": [s.summary() for s in stages],
    }


# ── 실행기 메모리 ─────────────────────────────────────────────

MEMORY_METRICS = {
    "jvm_heap": "JVMHeapMemory",
    "execution": "OnHeapExecutionMemory",
    "storage": "OnHeapStorageMemory",
    "gc_ms": "TotalGCTime",
}


def peak_memory(events) -> dict:
    """잡이 도는 동안 실행기가 찍은 최고 메모리.

    태스크가 끝날 때마다 그 실행기의 지표가 함께 남는다(`Task Executor Metrics`).
    전부 훑어 가장 큰 값을 고른다.

    셋을 나눠 보는 이유가 있다. `storage`는 캐시와 **브로드캐스트 블록**이 올라가는
    자리라 차원을 복사하는 비용이 여기 찍힌다. `execution`은 정렬과 해시 조인이
    쓰는 자리다. `jvm_heap`은 둘을 포함한 힙 전체인데 GC 시점에 따라 흔들린다.
    `gc_ms`는 실행기마다 누적값이라 최댓값이 곧 그 실행기의 총 GC 시간이다.
    """
    peaks = {key: 0 for key in MEMORY_METRICS}
    for line in events:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("Event") != TASK_END:
            continue
        metrics = event.get("Task Executor Metrics") or {}
        for key, field_name in MEMORY_METRICS.items():
            value = metrics.get(field_name, 0) or 0
            if value > peaks[key]:
                peaks[key] = value
    return peaks


# ── 물리 계획 ─────────────────────────────────────────────────

SQL_START = "org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart"
SQL_ADAPTIVE = ("org.apache.spark.sql.execution.ui."
                "SparkListenerSQLAdaptiveExecutionUpdate")
JOIN_OPERATORS = ("SortMergeJoin", "BroadcastHashJoin", "ShuffledHashJoin",
                  "BroadcastNestedLoopJoin", "CartesianProduct")


def final_plans(events) -> dict:
    """SQL 실행마다 마지막으로 기록된 물리 계획.

    실행이 시작할 때 계획이 한 번 남고, AQE가 켜져 있으면 단계가 끝날 때마다
    고친 계획이 다시 남는다. 마지막 것이 `isFinalPlan=true`인 최종 계획이다.

    형식은 `explain(mode="formatted")`와 같다. Spark가 UI에 보여줄 계획을
    `spark.sql.ui.explainMode`(기본 formatted)로 만들어 이벤트에 싣기 때문이다.
    그래서 실험을 다시 돌리지 않고도 **그때 실제로 돈 계획**을 꺼낼 수 있다.
    """
    plans = {}
    for line in events:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if event.get("Event") in (SQL_START, SQL_ADAPTIVE):
            plan = event.get("physicalPlanDescription")
            if plan:
                plans[event.get("executionId")] = plan
    return plans


def join_plan(events):
    """조인이 들어 있는 실행의 최종 계획. 없으면 None.

    한 잡에도 SQL 실행이 여럿이다. Delta가 트랜잭션 로그로 파일 목록을 만드는
    실행이 따로 있고, 그 계획이 본 쿼리보다 열 배 길다. 길이나 순서로 고르면
    그것이 뽑힌다. 조인 연산자가 있는 실행을 고른다.
    """
    for _, plan in sorted(final_plans(events).items()):
        if any(op in plan for op in JOIN_OPERATORS):
            return plan
    return None
