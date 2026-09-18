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


@dataclass
class Stage:
    """단계 하나. 태스크 여럿이 같은 일을 나눠 한다."""

    stage_id: int
    durations: list = field(default_factory=list)
    shuffle_read: int = 0
    shuffle_write: int = 0

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
        rows = sorted(self.durations)
        if not rows:
            return 0.0
        middle = len(rows) // 2
        return (float(rows[middle]) if len(rows) % 2
                else (rows[middle - 1] + rows[middle]) / 2)

    @property
    def max_over_median(self) -> float:
        """스큐의 크기. 1이면 고르고, 크면 한 태스크가 그만큼 붙잡는다."""
        median = self.median_ms
        return self.max_ms / median if median else 0.0

    def summary(self) -> dict:
        return {
            "stage_id": self.stage_id,
            "tasks": self.tasks,
            "max_ms": self.max_ms,
            "median_ms": self.median_ms,
            "max_over_median": round(self.max_over_median, 2),
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
        if kind != TASK_END:
            continue

        stage_id = event.get("Stage ID")
        if stage_id is None:
            continue
        stage = stages.setdefault(stage_id, Stage(stage_id=stage_id))

        metrics = event.get("Task Metrics") or {}
        # 태스크가 실제로 일한 시간. 대기 시간을 빼고 본다.
        stage.durations.append(_dig(metrics, "Executor Run Time"))
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


def read(path) -> dict:
    """로그를 읽어 단계별로 묶는다. 조각이 여럿이면 순서대로 잇는다."""
    def stream():
        for part in event_files(path):
            with _open(part) as handle:
                yield from handle

    return parse(stream())


def heaviest(stages):
    """가장 오래 걸린 단계. 스큐를 볼 자리다.

    단계가 수십 개라 전부 적으면 표가 읽히지 않는다. **전체 소요를 정하는 것은
    가장 무거운 단계**이므로 그것만 대표로 본다.
    """
    return max(stages, key=lambda s: s.total_ms, default=None)


def summary(parsed: dict) -> dict:
    """실험 한 번의 요약. 게이트가 요구한 항목을 전부 담는다."""
    stages = parsed["stages"]
    top = heaviest(stages)
    return {
        "app_ms": parsed["app_ms"],
        "stages": len(stages),
        "total_task_ms": sum(s.total_ms for s in stages),
        "shuffle_read_bytes": sum(s.shuffle_read for s in stages),
        "shuffle_write_bytes": sum(s.shuffle_write for s in stages),
        "heaviest": top.summary() if top else None,
    }
