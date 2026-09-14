"""중복 줄 중 어느 것을 남길지의 규칙.

규칙과 근거는 docs/deduplication.md에 있다. 여기서 고정하는 것은
**네 기준이 순서대로 적용되는가**와 **같은 입력에 같은 답이 나오는가**다.

뒤엣것이 특히 중요하다. 행 수만 보는 검증으로는 "매번 다른 줄을 남기는" 상태를
잡지 못한다. replaceWhere는 덤프 단위 교체까지만 보장하고,
같은 줄을 고르는 것은 이 규칙의 몫이다.
"""

import itertools

import pytest

from flux.silver import dedup
from flux.silver.dedup import Candidate
from flux.silver.mbid import MbidSource

A = "1557e001-08ec-405d-a0cf-6d2625e90af5"
B = "1850311f-b87c-4178-b98b-1d61487511c9"

EARLY = "listenbrainz-dump-2621-20260808-000003-incremental"
LATE = "listenbrainz-dump-2653-20260906-000002-incremental"


def c(record_id=1, dump=EARLY, source=MbidSource.NONE, artists=()):
    return Candidate(record_id=record_id, source_dump=dump,
                     mbid_source=source, artist_mbids=tuple(artists))


# ── 1순위: 출처 ─────────────────────────────────────────────

def test_출처가_좋은_쪽을_남긴다():
    kept = dedup.pick([c(1, source=MbidSource.LASTFM),
                       c(2, source=MbidSource.MAPPING)])
    assert kept.record_id == 2


@pytest.mark.parametrize("better,worse", [
    (MbidSource.MAPPING, MbidSource.CLIENT),
    (MbidSource.CLIENT, MbidSource.LASTFM),
    (MbidSource.LASTFM, MbidSource.NONE),
])
def test_출처_순위가_MAPPING_CLIENT_LASTFM_NONE이다(better, worse):
    kept = dedup.pick([c(1, source=worse), c(2, source=better)])
    assert kept.mbid_source is better


def test_출처가_나중_덤프보다_앞선다():
    """실측: 나중 덤프를 남기면 12.9%에서 MBID를 잃는다."""
    kept = dedup.pick([c(1, dump=EARLY, source=MbidSource.CLIENT),
                       c(2, dump=LATE, source=MbidSource.NONE)])
    assert kept.record_id == 1


def test_모르는_출처는_맨_뒤로_보낸다():
    """예외를 올리면 잡 하나가 레코드 한 건에 멈춘다."""
    kept = dedup.pick([c(1, source="이상한값"), c(2, source=MbidSource.NONE)])
    assert kept.record_id == 2


def test_전부_모르는_출처여도_하나를_고른다():
    kept = dedup.pick([c(1, source="가"), c(2, source="나")])
    assert kept.record_id == 1


# ── 2순위: 아티스트 개수 ─────────────────────────────────────

def test_출처가_같으면_아티스트가_많은_쪽():
    """사라지는 필드 1위가 lastfm_artist_mbid였다."""
    kept = dedup.pick([c(1, source=MbidSource.LASTFM, artists=[A]),
                       c(2, source=MbidSource.LASTFM, artists=[A, B])])
    assert kept.record_id == 2


def test_아티스트_개수가_나중_덤프보다_앞선다():
    kept = dedup.pick([c(1, dump=EARLY, artists=[A, B]),
                       c(2, dump=LATE, artists=[A])])
    assert kept.record_id == 1


def test_출처가_아티스트_개수보다_앞선다():
    """아티스트가 많아도 출처가 나쁘면 밀린다."""
    kept = dedup.pick([c(1, source=MbidSource.NONE, artists=[A, B]),
                       c(2, source=MbidSource.MAPPING, artists=[])])
    assert kept.record_id == 2


# ── 3순위: 덤프 ─────────────────────────────────────────────

def test_앞의_둘이_같으면_나중_덤프():
    kept = dedup.pick([c(1, dump=EARLY), c(2, dump=LATE)])
    assert kept.record_id == 2


def test_덤프가_record_id보다_앞선다():
    kept = dedup.pick([c(9, dump=LATE), c(1, dump=EARLY)])
    assert kept.record_id == 9


# ── 4순위: 동점을 가르는 마지막 못 ───────────────────────────

def test_전부_같으면_record_id가_작은_쪽():
    kept = dedup.pick([c(7), c(3), c(5)])
    assert kept.record_id == 3


def test_입력_순서가_달라도_같은_답이_나온다():
    """이것이 없으면 행 수는 같은데 값이 실행마다 달라진다."""
    rows = [c(7), c(3), c(5)]
    answers = {dedup.pick(list(order)).record_id
               for order in itertools.permutations(rows)}
    assert answers == {3}


def test_앞의_셋이_같은_경우에도_순서에_안_흔들린다():
    rows = [c(4, dump=LATE, source=MbidSource.CLIENT, artists=[A]),
            c(2, dump=LATE, source=MbidSource.CLIENT, artists=[A]),
            c(9, dump=LATE, source=MbidSource.CLIENT, artists=[A])]
    answers = {dedup.pick(list(order)).record_id
               for order in itertools.permutations(rows)}
    assert answers == {2}


# ── 경계 ───────────────────────────────────────────────────

def test_후보가_하나면_그것을_남긴다():
    assert dedup.pick([c(5)]).record_id == 5


def test_후보가_없으면_거부한다():
    """조용히 None을 돌려주면 그 청취가 통째로 사라진다."""
    with pytest.raises(ValueError):
        dedup.pick([])


def test_제너레이터도_받는다():
    kept = dedup.pick(c(i) for i in (4, 2, 8))
    assert kept.record_id == 2


def test_여덟_줄까지_처리한다():
    """실측에서 한 키의 최대가 8행이었다."""
    rows = [c(i, dump=EARLY) for i in range(8, 0, -1)]
    assert dedup.pick(rows).record_id == 1


# ── 지워지는 줄 ─────────────────────────────────────────────

def test_남긴_것_말고_전부_지워진다():
    rows = [c(1), c(2), c(3)]
    assert [d.record_id for d in dedup.dropped(rows)] == [2, 3]


def test_후보가_하나면_지워지는_것이_없다():
    assert dedup.dropped([c(1)]) == []


def test_지워지는_줄도_입력_순서에_안_흔들린다():
    rows = [c(7), c(3), c(5)]
    answers = {tuple(sorted(d.record_id for d in dedup.dropped(list(order))))
               for order in itertools.permutations(rows)}
    assert answers == {(5, 7)}


# ── Spark 쪽과 어긋나지 않게 ─────────────────────────────────

def test_정렬_명세가_순수_구현과_같은_순서다():
    """Window.orderBy를 이 명세로 만든다. 둘이 갈리면 결과가 달라진다."""
    assert [name for name, _ in dedup.ORDER] == [
        "_mbid_rank", "_artist_count", "_source_dump", "_record_id"]


def test_정렬_방향이_규칙과_맞다():
    assert [direction for _, direction in dedup.ORDER] == [
        dedup.ASC, dedup.DESC, dedup.DESC, dedup.ASC]


def test_키는_명세_6_3을_따른다():
    assert dedup.KEY == ("user_name", "listened_at", "recording_msid")
