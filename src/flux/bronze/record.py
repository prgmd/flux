"""원본 한 줄을 Bronze 행으로 바꾼다.

원칙은 하나다. **`raw_json`을 변형하지 않는다.**
파싱은 값을 몇 개 뽑아내기 위한 것이지 저장할 형태를 만들기 위한 것이 아니다.
정규화와 검증은 Silver의 일이다.
"""

import json
from dataclasses import dataclass
from datetime import date, datetime, timezone

MAX_ERROR_LEN = 300


@dataclass(frozen=True)
class Row:
    """적재 가능한 한 행."""
    _ingested_at: datetime
    _source_dump: str
    _source_file: str
    _record_id: int
    listened_at: int | None
    user_name: str | None
    raw_json: str
    ingested_date: date

    def as_dict(self) -> dict:
        return {
            "_ingested_at": self._ingested_at,
            "_source_dump": self._source_dump,
            "_source_file": self._source_file,
            "_record_id": self._record_id,
            "listened_at": self.listened_at,
            "user_name": self.user_name,
            "raw_json": self.raw_json,
            "ingested_date": self.ingested_date,
        }


@dataclass(frozen=True)
class Rejected:
    """해석하지 못해 격리 테이블로 보낼 한 행."""
    _ingested_at: datetime
    _source_dump: str
    _source_file: str
    _record_id: int
    raw_line: str
    error: str
    ingested_date: date

    def as_dict(self) -> dict:
        return {
            "_ingested_at": self._ingested_at,
            "_source_dump": self._source_dump,
            "_source_file": self._source_file,
            "_record_id": self._record_id,
            "raw_line": self.raw_line,
            "error": self.error,
            "ingested_date": self.ingested_date,
        }


def _decode(line) -> str:
    if isinstance(line, bytes):
        return line.decode("utf-8", errors="replace")
    return line


def _as_int(value) -> int | None:
    """정수로 볼 수 있으면 정수로. 아니면 None.

    실패를 예외로 만들지 않는다. 파생 컬럼이 비는 것은 격리 사유가 아니고,
    원본은 raw_json에 그대로 남아 Silver에서 다시 볼 수 있다.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _as_str(value) -> str | None:
    return value if isinstance(value, str) else None


def build(
    line,
    *,
    source_dump: str,
    source_file: str,
    index: int,
    ingested_at: datetime | None = None,
) -> Row | Rejected:
    """한 줄을 Row나 Rejected로 만든다. 예외를 밖으로 던지지 않는다.

    한 줄이 깨졌다고 적재 전체가 멈추면 안 된다. 깨진 줄은 격리하고 나머지를 계속 넣는다.
    """
    ingested_at = ingested_at or datetime.now(timezone.utc)
    ingested_date = ingested_at.date()
    text = _decode(line)

    def reject(reason: str) -> Rejected:
        return Rejected(
            _ingested_at=ingested_at,
            _source_dump=source_dump,
            _source_file=source_file,
            _record_id=index,
            raw_line=text,
            error=reason[:MAX_ERROR_LEN],
            ingested_date=ingested_date,
        )

    stripped = text.strip()
    if not stripped:
        return reject("빈 줄")

    try:
        parsed = json.loads(stripped)
    except json.JSONDecodeError as exc:
        return reject(f"JSON 파싱 실패: {exc}")

    if not isinstance(parsed, dict):
        return reject(f"최상위가 객체가 아니다: {type(parsed).__name__}")

    return Row(
        _ingested_at=ingested_at,
        _source_dump=source_dump,
        _source_file=source_file,
        _record_id=index,
        listened_at=_as_int(parsed.get("listened_at", parsed.get("timestamp"))),
        user_name=_as_str(parsed.get("user_name")),
        raw_json=stripped,
        ingested_date=ingested_date,
    )
