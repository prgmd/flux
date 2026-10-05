"""잡 실행 기록. 기록의 모양과, 감싼 잡이 어떻게 끝나든 한 줄이 남는가. Spark 없이 돈다."""

from datetime import datetime, timezone

import pytest

from flux.common import config, job_runs, runs

AT = datetime(2026, 10, 5, 1, 0, tzinfo=timezone.utc)


def _record(**overrides):
    values = dict(run_id="r", started_at=AT, finished_at=AT.replace(minute=2),
                  exit_code=0, arguments=["--dump", "d"], measurements=["m"], error=None)
    values.update(overrides)
    return runs.record("job", **values)


def test_종료_코드_0이고_예외가_없으면_성공이다():
    assert _record()["status"] == runs.SUCCEEDED


def test_확인_실패로_1을_돌려주면_실패다():
    """잡들은 대조가 어긋나면 예외 대신 1을 돌려준다."""
    assert _record(exit_code=1)["status"] == runs.FAILED


def test_예외로_끝나면_실패다():
    assert _record(exit_code=None, error="ValueError: x")["status"] == runs.FAILED


def test_걸린_시간과_날짜를_채운다():
    row = _record()
    assert row["seconds"] == 120.0
    assert row["run_date"] == AT.date()


def test_긴_오류는_자른다():
    assert len(_record(exit_code=None, error="x" * 2000)["error"]) == runs.MAX_ERROR


@pytest.fixture
def captured(monkeypatch, tmp_path):
    monkeypatch.setenv(config.STORAGE_ROOT_ENV, tmp_path.as_posix())
    rows = []
    return rows


def test_성공한_잡도_한_줄_남긴다(captured):
    assert job_runs.run(lambda: 0, job="j", append=captured.append) == 0
    assert [r["status"] for r in captured] == [runs.SUCCEEDED]


def test_1을_돌려준_잡은_실패로_남긴다(captured):
    assert job_runs.run(lambda: 1, job="j", append=captured.append) == 1
    assert [r["status"] for r in captured] == [runs.FAILED]


def test_예외로_죽은_잡도_한_줄_남기고_예외를_그대로_올린다(captured):
    def boom():
        raise ValueError("깨졌다")

    with pytest.raises(ValueError):
        job_runs.run(boom, job="j", append=captured.append)
    assert captured[0]["status"] == runs.FAILED
    assert "ValueError: 깨졌다" in captured[0]["error"]


def test_이번에_남긴_측정만_가리킨다(captured):
    config.write_measurement("before-run", {})

    def main():
        config.write_measurement("during-run", {})
        return 0

    job_runs.run(main, job="j", append=captured.append)
    assert captured[0]["measurements"] == '["during-run"]'


def test_기록을_못_남겨도_잡의_결과는_그대로다(captured):
    def broken(row):
        raise OSError("디스크가 없다")

    assert job_runs.run(lambda: 0, job="j", append=broken) == 0
