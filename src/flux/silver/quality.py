"""레코드를 통과시킬지 격리할지 정한다.

명세 6.3의 품질 규칙 넷 중 셋이 여기 있다. 넷째(MBID 형식)는 성격이 달라
mbid.resolve 안에 들어 있다. 레코드를 버리지 않고 그 필드만 비우기 때문이다.

    1. listened_at이 미래 또는 1970 이전      → 격리
    2. user_name / listened_at 누락            → 격리
    3. track_metadata 없음                     → 격리

격리는 버리는 것이 아니다. 왜 걸렸는지를 함께 적어 따로 쌓아 두고,
그 분포가 품질 리포트의 재료가 된다. 비율이 갑자기 변하면 원천이 바뀐 것이다.

pyspark를 부르지 않는다. "이 레코드가 성한가"는 Spark가 필요 없는 판단이다.
"""

from dataclasses import dataclass

EPOCH_1970 = 0


class Reason:
    """격리 사유. 문자열을 그대로 쓰면 오타가 조용히 새 사유를 만든다."""

    LISTENED_AT_MISSING = "청취 시각 없음"
    LISTENED_AT_BEFORE_1970 = "청취 시각이 1970 이전"
    LISTENED_AT_FUTURE = "청취 시각이 적재 시각보다 미래"
    USER_NAME_MISSING = "사용자 이름 없음"
    METADATA_MISSING = "track_metadata 없음"


@dataclass(frozen=True)
class Verdict:
    """통과 여부와 사유. 사유는 격리된 경우에만 채워진다."""

    ok: bool
    reason: str | None = None


PASS = Verdict(True)


def _blank(value) -> bool:
    """없거나 공백뿐인가. 빈 문자열은 값이 아니다."""
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def _as_int(value):
    """정수로 볼 수 있으면 정수로. 아니면 None.

    bool을 거르는 이유는 파이썬에서 bool이 int의 하위형이라
    True가 1로 통과해 버리기 때문이다.
    """
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value.strip())
        except ValueError:
            return None
    return None


def listened_at_of(document):
    """청취 시각을 꺼낸다. 이름이 둘이라 앞의 것을 먼저 본다.

    명세는 listened_at이라고 부르지만 덤프의 필드 이름은 timestamp다.
    둘 다 보는 이유는 원천이 이름을 바꿔도 견디기 위해서다.
    """
    if not isinstance(document, dict):
        return None
    for key in ("listened_at", "timestamp"):
        found = _as_int(document.get(key))
        if found is not None:
            return found
    return None


def judge(document, *, ingested_at) -> Verdict:
    """레코드 하나를 판정한다. 먼저 걸리는 사유 하나만 돌려준다.

    사유를 여럿 모으지 않는 이유는 격리 분포를 세기 위해서다.
    한 행이 여러 사유에 걸리면 합계가 행 수와 맞지 않는다.

    ingested_at은 밖에서 받는다. 지금 시각으로 재면 같은 데이터를 다시 판정할 때
    결과가 달라진다. 미래인지는 **그 행이 적재된 시각**을 기준으로 본다.
    """
    if not isinstance(document, dict):
        return Verdict(False, Reason.METADATA_MISSING)

    if _blank(document.get("user_name")):
        return Verdict(False, Reason.USER_NAME_MISSING)

    listened_at = listened_at_of(document)
    if listened_at is None:
        return Verdict(False, Reason.LISTENED_AT_MISSING)
    if listened_at < EPOCH_1970:
        return Verdict(False, Reason.LISTENED_AT_BEFORE_1970)
    if listened_at > ingested_at:
        return Verdict(False, Reason.LISTENED_AT_FUTURE)

    if not isinstance(document.get("track_metadata"), dict):
        return Verdict(False, Reason.METADATA_MISSING)

    return PASS
