"""MBID 우선순위 해석.

규칙과 근거는 docs/mbid_resolution.md에 있다. 여기서 고정하는 것은
**어느 자리를 먼저 보는가**와 **형식이 틀렸을 때 무엇을 버리는가**다.

픽스처는 실제 덤프에서 본 모양을 그대로 쓴다. 상상한 형식으로 테스트하면
테스트만 통과하고 실물에서 깨진다.
"""

import pytest

from flux.silver import mbid
from flux.silver.mbid import MbidSource

MAPPING_MBID = "084734a4-4518-470b-b92b-f053d8e618f1"
CLIENT_MBID = "379217d3-0c05-4cbe-9205-0fcb618246d4"
LASTFM_MBID = "f34d4227-160c-4dc6-a50a-c3fd37bc006c"
ARTIST_A = "1557e001-08ec-405d-a0cf-6d2625e90af5"
ARTIST_B = "1850311f-b87c-4178-b98b-1d61487511c9"


def doc(*, mapping=None, info=None):
    metadata = {"track_name": "Home To You", "artist_name": "The Neighborly"}
    if mapping is not None:
        metadata["mbid_mapping"] = mapping
    metadata["additional_info"] = info if info is not None else {}
    return {
        "user_name": "Pfefan",
        "timestamp": 1788393521,
        "recording_msid": "bdbbcdc8-6d56-4915-b10c-623d08a9c101",
        "track_metadata": metadata,
    }


# ── 우선순위 ────────────────────────────────────────────────

def test_mapping이_있으면_mapping이다():
    r = mbid.resolve(doc(mapping={"recording_mbid": MAPPING_MBID}))
    assert r.source is MbidSource.MAPPING
    assert r.recording_mbid == MAPPING_MBID


def test_mapping이_없으면_클라이언트다():
    r = mbid.resolve(doc(info={"recording_mbid": CLIENT_MBID}))
    assert r.source is MbidSource.CLIENT
    assert r.recording_mbid == CLIENT_MBID


def test_둘_다_없으면_lastfm이다():
    """실측에서 32.85%가 여기 걸린다. 명세는 이 자리를 보지 않았다."""
    r = mbid.resolve(doc(info={"lastfm_track_mbid": LASTFM_MBID}))
    assert r.source is MbidSource.LASTFM
    assert r.recording_mbid == LASTFM_MBID


def test_아무_자리에도_없으면_NONE이다():
    r = mbid.resolve(doc())
    assert r.source is MbidSource.NONE
    assert r.recording_mbid is None


def test_mapping이_클라이언트보다_앞선다():
    r = mbid.resolve(doc(mapping={"recording_mbid": MAPPING_MBID},
                         info={"recording_mbid": CLIENT_MBID}))
    assert r.recording_mbid == MAPPING_MBID


def test_클라이언트가_lastfm보다_앞선다():
    r = mbid.resolve(doc(info={"recording_mbid": CLIENT_MBID,
                               "lastfm_track_mbid": LASTFM_MBID}))
    assert r.source is MbidSource.CLIENT
    assert r.recording_mbid == CLIENT_MBID


def test_mapping은_track_metadata_아래에서_찾는다():
    """최상위에 둔 값은 무시한다. 실측에서 최상위는 0행이었다."""
    document = doc()
    document["mbid_mapping"] = {"recording_mbid": MAPPING_MBID}
    assert mbid.resolve(document).source is MbidSource.NONE


# ── 형식이 틀렸을 때 ─────────────────────────────────────────

@pytest.mark.parametrize("bad", [
    "not-a-uuid", "", "   ", "084734a4451847", None, 42, [],
    "084734a4-4518-470b-b92b-f053d8e618f",     # 한 자 짧다
    "084734a4-4518-470b-b92b-f053d8e618f1x",   # 한 자 길다
    "084734a4_4518_470b_b92b_f053d8e618f1",    # 구분자가 다르다
    "zz4734a4-4518-470b-b92b-f053d8e618f1",    # 16진수가 아니다
])
def test_형식이_틀리면_그_자리는_없는_것이다(bad):
    r = mbid.resolve(doc(mapping={"recording_mbid": bad},
                         info={"recording_mbid": CLIENT_MBID}))
    assert r.source is MbidSource.CLIENT


def test_형식이_틀려도_레코드를_버리지_않는다():
    """명세 6.3: MBID가 UUID 형식이 아니면 해당 필드만 NULL, 레코드는 통과."""
    r = mbid.resolve(doc(info={"recording_mbid": "깨진값"}))
    assert r.source is MbidSource.NONE
    assert r.recording_mbid is None


def test_대문자는_소문자로_맞춘다():
    """같은 MBID가 대소문자만 달라 다른 값으로 세어지면 안 된다."""
    r = mbid.resolve(doc(info={"recording_mbid": CLIENT_MBID.upper()}))
    assert r.recording_mbid == CLIENT_MBID


def test_앞뒤_공백을_턴다():
    r = mbid.resolve(doc(info={"recording_mbid": f"  {CLIENT_MBID}  "}))
    assert r.recording_mbid == CLIENT_MBID


# ── 아티스트 ────────────────────────────────────────────────

def test_아티스트_목록을_가져온다():
    r = mbid.resolve(doc(mapping={"recording_mbid": MAPPING_MBID,
                                  "artist_mbids": [ARTIST_A, ARTIST_B]}))
    assert r.artist_mbids == (ARTIST_A, ARTIST_B)


def test_아티스트_하나가_깨져도_나머지를_남긴다():
    """아티스트가 여럿인 곡에서 하나 때문에 전부 잃으면 손해가 더 크다."""
    r = mbid.resolve(doc(mapping={"recording_mbid": MAPPING_MBID,
                                  "artist_mbids": ["깨짐", ARTIST_B]}))
    assert r.artist_mbids == (ARTIST_B,)


def test_lastfm_아티스트는_문자열_하나로_온다():
    """lastfm_artist_mbid는 목록이 아니라 값 하나다. 실제 덤프의 모양이다."""
    r = mbid.resolve(doc(info={"lastfm_track_mbid": LASTFM_MBID,
                               "lastfm_artist_mbid": ARTIST_A}))
    assert r.artist_mbids == (ARTIST_A,)


def test_recording이_없어도_아티스트는_살린다():
    """recording MBID가 없다고 아티스트 MBID까지 버릴 이유가 없다."""
    r = mbid.resolve(doc(info={"artist_mbids": [ARTIST_A]}))
    assert r.source is MbidSource.NONE
    assert r.artist_mbids == (ARTIST_A,)


def test_아티스트가_없으면_빈_튜플이다():
    r = mbid.resolve(doc(info={"recording_mbid": CLIENT_MBID}))
    assert r.artist_mbids == ()


# ── 문서가 예상과 다를 때 ────────────────────────────────────

@pytest.mark.parametrize("document", [
    {},
    {"track_metadata": None},
    {"track_metadata": {}},
    {"track_metadata": {"additional_info": None}},
    {"track_metadata": {"mbid_mapping": "문자열"}},
    {"track_metadata": "문자열"},
])
def test_구조가_달라도_죽지_않는다(document):
    """Bronze는 판단하지 않고 담기만 한다. 이상한 문서가 여기까지 온다."""
    r = mbid.resolve(document)
    assert r.source is MbidSource.NONE


# ── 제출 클라이언트 ──────────────────────────────────────────

def test_클라이언트_이름을_그대로_읽는다():
    assert mbid.submission_client(
        doc(info={"submission_client": "ListenBrainz lastfm importer v2"})
    ) == "ListenBrainz lastfm importer v2"


@pytest.mark.parametrize("bad", [None, "", "   ", 42, {}])
def test_클라이언트가_없으면_UNKNOWN이다(bad):
    """추론하지 않는다. 모르는 것을 모른다고 적어야 나중에 셀 수 있다."""
    assert mbid.submission_client(doc(info={"submission_client": bad})) == "UNKNOWN"


def test_클라이언트_이름의_공백을_턴다():
    assert mbid.submission_client(doc(info={"submission_client": " navidrome "})) \
        == "navidrome"


def test_문서가_비어도_UNKNOWN이다():
    assert mbid.submission_client({}) == "UNKNOWN"
