"""날짜 차원과 사용자 차원의 규칙. Spark 없이 돈다."""

from datetime import date, datetime

import pytest

from flux.gold import dates


def test_월요일이_1이고_일요일이_7이다():
    assert dates.attributes(date(2026, 10, 5))["day_of_week"] == 1
    assert dates.attributes(date(2026, 10, 4))["day_of_week"] == 7


def test_주말은_토요일과_일요일이다():
    weekend = [dates.attributes(date(2026, 10, d))["is_weekend"] for d in range(5, 12)]
    assert weekend == [False, False, False, False, False, True, True]


def test_1월_1일이_금요일이면_앞해의_53주다():
    """2027-01-01은 금요일이다. 그해의 첫 목요일(1월 7일)이 든 주가 1주다."""
    row = dates.attributes(date(2027, 1, 1))
    assert (row["year"], row["iso_year"], row["iso_week"]) == (2027, 2026, 53)
    assert row["week_start"] == date(2026, 12, 28)


def test_12월_말이_다음해의_1주일_수_있다():
    """2025-12-29는 월요일이고, 그 주의 목요일이 2026-01-01이라 2026년 1주다."""
    row = dates.attributes(date(2025, 12, 29))
    assert (row["year"], row["iso_year"], row["iso_week"]) == (2025, 2026, 1)


def test_연월은_팩트의_파티션_값과_같은_모양이다():
    assert dates.attributes(date(2026, 9, 30))["listened_month"] == "2026-09"


def test_기간은_양끝을_포함한다():
    days = dates.span(date(2026, 2, 27), date(2026, 3, 1))
    assert days == [date(2026, 2, 27), date(2026, 2, 28), date(2026, 3, 1)]


def test_하루짜리_기간도_한_줄이다():
    assert dates.span(date(2026, 1, 1), date(2026, 1, 1)) == [date(2026, 1, 1)]


def test_거꾸로_된_기간은_거부한다():
    with pytest.raises(ValueError):
        dates.span(date(2026, 1, 2), date(2026, 1, 1))


def test_가장_최근_청취의_이름을_고른다():
    rows = [(datetime(2026, 1, 1), "old"), (datetime(2026, 9, 1), "new"),
            (datetime(2026, 5, 1), "middle")]
    assert dates.current_name(rows) == "new"


def test_같은_시각이면_사전순으로_뒤의_이름이다():
    at = datetime(2026, 9, 1)
    assert dates.current_name([(at, "alpha"), (at, "beta")]) == "beta"
    assert dates.current_name([(at, "beta"), (at, "alpha")]) == "beta"
