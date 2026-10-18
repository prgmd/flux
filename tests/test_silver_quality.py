"""품질 규칙 — 무엇을 격리하고 무엇을 통과시키는가.

격리는 버리는 것이 아니라 따로 쌓는 것이므로, **왜 걸렸는지**가 결과의 일부다.
사유가 흔들리면 품질 리포트가 의미를 잃는다.

한 행이 여러 사유에 걸릴 수 있으므로 **어느 사유가 먼저인지**도 고정한다.
사유를 여럿 돌려주면 격리 분포의 합이 행 수와 맞지 않는다.
"""

import pytest

from flux.silver import quality
from flux.silver.quality import Reason

INGESTED = 1788393600          # 2026-09-14 언저리
LISTENED = 1675677878          # 2023-02-06


def doc(**overrides):
    base = {
        "user_id": 3277,
        "user_name": "Izum",
        "timestamp": LISTENED,
        "recording_msid": "bdbbcdc8-6d56-4915-b10c-623d08a9c101",
        "track_metadata": {
            "track_name": "Home To You",
            "artist_name": "The Neighborly",
            "additional_info": {},
        },
    }
    base.update(overrides)
    return base


def judge(document):
    return quality.judge(document, ingested_at=INGESTED)


# ── 통과 ───────────────────────────────────────────────────

def test_성한_레코드는_통과한다():
    assert judge(doc()).ok is True


def test_통과하면_사유가_없다():
    assert judge(doc()).reason is None


def test_적재_시각과_같은_청취는_통과한다():
    """경계는 미래가 아니다. 같은 초에 듣고 제출할 수 있다."""
    assert judge(doc(timestamp=INGESTED)).ok is True


def test_정확히_1970이면_통과한다():
    """규칙은 '1970 이전'이다. 1970-01-01 정각은 이전이 아니다."""
    assert judge(doc(timestamp=0)).ok is True


# ── 규칙 2: 필수값 누락 ──────────────────────────────────────

@pytest.mark.parametrize("value", [None, "", "   ", "\t"])
def test_사용자_이름이_비면_격리한다(value):
    verdict = judge(doc(user_name=value))
    assert verdict.ok is False
    assert verdict.reason == Reason.USER_NAME_MISSING


def test_사용자_이름_키가_아예_없어도_격리한다():
    document = doc()
    del document["user_name"]
    assert judge(document).reason == Reason.USER_NAME_MISSING


def test_청취_시각이_없으면_격리한다():
    document = doc()
    del document["timestamp"]
    assert judge(document).reason == Reason.LISTENED_AT_MISSING


@pytest.mark.parametrize("bad", ["어제", "", None, [], {}, True])
def test_청취_시각이_숫자가_아니면_없는_것이다(bad):
    """True는 파이썬에서 int의 하위형이라 걸러내지 않으면 1로 통과한다."""
    assert judge(doc(timestamp=bad)).reason == Reason.LISTENED_AT_MISSING


def test_문자열로_온_숫자는_받는다():
    """원천이 타입을 바꿔도 견딘다."""
    assert judge(doc(timestamp=str(LISTENED))).ok is True


# ── 규칙 1: 청취 시각의 범위 ─────────────────────────────────

def test_1970_이전은_격리한다():
    verdict = judge(doc(timestamp=-1))
    assert verdict.reason == Reason.LISTENED_AT_BEFORE_1970


def test_적재_시각보다_미래면_격리한다():
    verdict = judge(doc(timestamp=INGESTED + 1))
    assert verdict.reason == Reason.LISTENED_AT_FUTURE


def test_미래_판정은_지금_시각이_아니라_적재_시각_기준이다():
    """지금으로 재면 같은 데이터를 다시 판정할 때 결과가 달라진다."""
    document = doc(timestamp=INGESTED + 100)
    assert quality.judge(document, ingested_at=INGESTED).ok is False
    assert quality.judge(document, ingested_at=INGESTED + 200).ok is True


# ── 규칙 3: track_metadata ──────────────────────────────────

def test_메타데이터가_없으면_격리한다():
    document = doc()
    del document["track_metadata"]
    assert judge(document).reason == Reason.METADATA_MISSING


@pytest.mark.parametrize("bad", [None, "문자열", 42, []])
def test_메타데이터가_객체가_아니면_격리한다(bad):
    assert judge(doc(track_metadata=bad)).reason == Reason.METADATA_MISSING


def test_메타데이터가_비어도_통과한다():
    """규칙은 '없음'이다. 비어 있는 것과 없는 것은 다르다."""
    assert judge(doc(track_metadata={})).ok is True


# ── 문서 자체가 이상할 때 ────────────────────────────────────

@pytest.mark.parametrize("bad", [None, "문자열", 42, [], ()])
def test_문서가_객체가_아니면_격리한다(bad):
    assert judge(bad).reason == Reason.METADATA_MISSING


# ── 사유의 순서 ────────────────────────────────────────────

def test_사유는_하나만_돌려준다():
    """여럿 돌려주면 격리 분포의 합이 행 수와 맞지 않는다."""
    document = doc(user_name=None, timestamp=None, track_metadata=None)
    assert isinstance(judge(document).reason, str)


def test_이름이_시각보다_먼저_걸린다():
    document = doc(user_name=None, timestamp=None)
    assert judge(document).reason == Reason.USER_NAME_MISSING


def test_시각이_메타데이터보다_먼저_걸린다():
    document = doc(timestamp=None, track_metadata=None)
    assert judge(document).reason == Reason.LISTENED_AT_MISSING


def test_없는_시각이_범위보다_먼저_걸린다():
    """없는 것을 범위로 판정할 수는 없다."""
    assert judge(doc(timestamp=None)).reason == Reason.LISTENED_AT_MISSING


# ── 청취 시각을 꺼내는 규칙 ──────────────────────────────────

def test_listened_at이_timestamp보다_앞선다():
    """명세는 listened_at, 덤프는 timestamp. 둘 다 오면 명세 이름을 따른다."""
    document = doc(timestamp=1)
    document["listened_at"] = LISTENED
    assert quality.listened_at_of(document) == LISTENED


def test_timestamp만_있으면_그것을_쓴다():
    assert quality.listened_at_of(doc()) == LISTENED


def test_둘_다_없으면_None이다():
    document = doc()
    del document["timestamp"]
    assert quality.listened_at_of(document) is None


def test_앞의_이름이_못_쓸_값이면_뒤를_본다():
    """listened_at이 깨졌다고 멀쩡한 timestamp를 버릴 이유가 없다."""
    document = doc(timestamp=LISTENED)
    document["listened_at"] = "깨짐"
    assert quality.listened_at_of(document) == LISTENED

def test_재생_시간이_10초_미만이면_격리한다():
    document = doc()
    document["track_metadata"]["additional_info"]["ms_played"] = 9999
    verdict = judge(document)
    assert verdict.ok is False
    assert verdict.reason == "재생 시간이 10초 미만"

# ── 격리 드릴의 불량 견본 ───────────────────────────────────

def test_불량_견본은_규칙마다_하나씩_네_종류다():
    """격리 드릴이 Bronze에 넣는 행이다. 규칙 넷을 하나씩 건드린다."""
    samples = quality.spoiled(doc(), ingested_at=INGESTED)
    assert sorted(s.reason for s in samples) == sorted([
        Reason.USER_NAME_MISSING, Reason.LISTENED_AT_BEFORE_1970,
        Reason.LISTENED_AT_FUTURE, Reason.METADATA_MISSING,
    ])


def test_불량_견본은_순수_판정에서도_그_사유로_걸린다():
    """드릴은 Spark가 이 사유를 붙이는지 본다. 기대값이 순수 판정과 같아야
    Spark와 순수 규칙이 갈라졌을 때 드릴이 잡는다."""
    for sample in quality.spoiled(doc(), ingested_at=INGESTED):
        verdict = quality.judge(sample.document, ingested_at=INGESTED)
        assert verdict == quality.Verdict(False, sample.reason), sample.label


def test_불량_견본의_원본은_성한_레코드여야_한다():
    """원본부터 걸리면 견본이 무엇 때문에 걸렸는지 알 수 없다."""
    with pytest.raises(ValueError):
        quality.spoiled(doc(user_name=""), ingested_at=INGESTED)


def test_불량_견본은_원본을_바꾸지_않는다():
    original = doc()
    before = repr(original)
    quality.spoiled(original, ingested_at=INGESTED)
    assert repr(original) == before


def test_시각_견본은_원본이_쓰는_키를_고친다():
    """덤프는 timestamp를 쓴다. listened_at 키를 새로 붙이면 원본과 모양이 달라진다."""
    samples = {s.reason: s for s in quality.spoiled(doc(), ingested_at=INGESTED)}
    early = samples[Reason.LISTENED_AT_BEFORE_1970].document
    assert "listened_at" not in early and early["timestamp"] < 0
