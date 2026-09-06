"""recent_dumps — 최근 N일 창으로 덤프를 고르는 규칙.

창을 오늘이 아니라 목록의 최신 덤프에 맞추는 판단이 핵심이라
그 차이가 드러나는 경우를 골라 고정한다.
"""

from datetime import date

import pytest

from flux.ingest import catalog
from flux.ingest.catalog import DumpKind


def make(day: int, serial: int = 0, month: int = 9) -> catalog.Dump:
    name = f"listenbrainz-dump-{serial or day}-2026{month:02d}{day:02d}-000002-incremental"
    return catalog.parse_dump_name(name)


def test_창_안의_덤프만_고른다():
    dumps = [make(d) for d in range(1, 11)]
    picked = catalog.recent_dumps(dumps, days=3)
    assert [d.dumped_on.day for d in picked] == [8, 9, 10]


def test_기준은_오늘이_아니라_최신_덤프다():
    """서버가 오늘치를 아직 안 올렸을 때 하나를 잃지 않아야 한다."""
    dumps = [make(d) for d in range(1, 6)]      # 9/1 ~ 9/5
    picked = catalog.recent_dumps(dumps, days=2)
    # 오늘(9/7) 기준이면 9/6~9/7 창이 되어 아무것도 안 걸린다.
    assert [d.dumped_on.day for d in picked] == [4, 5]


def test_기준일을_직접_줄_수_있다():
    dumps = [make(d) for d in range(1, 11)]
    picked = catalog.recent_dumps(dumps, days=2, anchor=date(2026, 9, 5))
    assert [d.dumped_on.day for d in picked] == [4, 5]


def test_창이_목록보다_넓으면_전부_돌려준다():
    dumps = [make(d) for d in range(1, 4)]
    assert len(catalog.recent_dumps(dumps, days=365)) == 3


def test_일련번호가_건너뛰어도_날짜로_고른다():
    """외부 식별자에 규칙을 기대하지 않는다. 창은 날짜로만 정한다."""
    dumps = [make(1, serial=2646), make(2, serial=2649), make(3, serial=2650)]
    picked = catalog.recent_dumps(dumps, days=2)
    assert [d.serial for d in picked] == [2649, 2650]


def test_결과는_항상_오래된_것부터다():
    dumps = [make(3), make(1), make(2)]
    picked = catalog.recent_dumps(dumps, days=3)
    assert [d.dumped_on.day for d in picked] == [1, 2, 3]


def test_같은_날_두_덤프도_모두_고른다():
    a = catalog.parse_dump_name("listenbrainz-dump-1-20260901-000002-incremental")
    b = catalog.parse_dump_name("listenbrainz-dump-2-20260901-120000-incremental")
    picked = catalog.recent_dumps([a, b], days=1)
    assert [d.serial for d in picked] == [1, 2]


def test_빈_목록은_빈_결과다():
    assert catalog.recent_dumps([], days=30) == []


def test_days가_0_이하면_거부한다():
    with pytest.raises(ValueError):
        catalog.recent_dumps([make(1)], days=0)


def test_달을_넘어가는_창도_날짜로_계산한다():
    dumps = [make(30, month=8), make(31, month=8), make(1), make(2)]
    picked = catalog.recent_dumps(dumps, days=3)
    assert [(d.dumped_on.month, d.dumped_on.day) for d in picked] == [
        (8, 31), (9, 1), (9, 2)
    ]


def test_종류가_섞여_있어도_창은_날짜만_본다():
    full = catalog.parse_dump_name("listenbrainz-dump-9-20260902-000002-full")
    dumps = [make(1), make(2), full]
    picked = catalog.recent_dumps(dumps, days=1)
    assert {d.kind for d in picked} == {DumpKind.INCREMENTAL, DumpKind.FULL}


def test_일련번호가_날짜와_반대로_붙어도_날짜를_따른다():
    """정렬을 일련번호에 맡기면 기준점이 엉뚱한 곳에 잡힌다.

    실제로 이 테스트를 쓰다 sorted(dumps)가 serial부터 비교한다는 것을 발견했다.
    """
    dumps = [make(1, serial=999), make(2, serial=1)]
    picked = catalog.recent_dumps(dumps, days=1)
    assert [d.serial for d in picked] == [1]
