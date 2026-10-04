"""여러 날치 적재의 계획 로직.

Spark도 네트워크도 없이 검증할 수 있는 부분만 여기서 고정한다.
실제 적재가 맞는지는 대조 불변식이 보고, 여기서는 **무엇을 건너뛰는가**만 본다.
건너뛰기를 잘못하면 하루치가 통째로 비므로 실패 기록의 취급이 핵심이다.
"""

import json

from flux.bronze import backfill


def write(directory, dump, **fields):
    payload = {"dump": dump, "reconciled": True}
    payload.update(fields)
    path = backfill.measurement_path(directory, dump)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


# ── 무엇을 끝난 것으로 볼 것인가 ──────────────────────────────

def test_대조를_통과한_기록만_끝난_것이다(tmp_path):
    write(tmp_path, "dump-a")
    write(tmp_path, "dump-b", reconciled=False)
    assert backfill.reconciled_dumps(tmp_path) == {"dump-a"}


def test_대조_필드가_없으면_끝난_것이_아니다(tmp_path):
    path = backfill.measurement_path(tmp_path, "dump-a")
    path.write_text(json.dumps({"dump": "dump-a"}), encoding="utf-8")
    assert backfill.reconciled_dumps(tmp_path) == set()


def test_깨진_기록은_무시하고_나머지를_살린다(tmp_path):
    """한 파일이 깨졌다고 남은 29일치를 멈추지 않는다."""
    backfill.measurement_path(tmp_path, "dump-a").write_text("{잘림", encoding="utf-8")
    write(tmp_path, "dump-b")
    assert backfill.reconciled_dumps(tmp_path) == {"dump-b"}


def test_기록_폴더가_없으면_빈_집합이다(tmp_path):
    assert backfill.reconciled_dumps(tmp_path / "없음") == set()


def test_다른_이름의_json은_보지_않는다(tmp_path):
    (tmp_path / "listens-density.json").write_text(
        json.dumps({"dump": "dump-a", "reconciled": True}), encoding="utf-8"
    )
    assert backfill.reconciled_dumps(tmp_path) == set()


def test_파일_이름이_아니라_안의_이름을_믿는다(tmp_path):
    """파일 이름은 사람이 바꿀 수 있다. 적재가 적어 둔 이름이 정본이다."""
    path = backfill.measurement_path(tmp_path, "겉이름")
    path.write_text(json.dumps({"dump": "속이름", "reconciled": True}), encoding="utf-8")
    assert backfill.reconciled_dumps(tmp_path) == {"속이름"}


# ── 무엇을 할 것인가 ─────────────────────────────────────────

def test_끝난_덤프는_걸음에서_빠진다(tmp_path):
    archives = [("a", tmp_path / "a.tar"), ("b", tmp_path / "b.tar")]
    steps = backfill.plan(archives, tmp_path / "staging", done={"a"})
    assert [s.name for s in steps] == ["b"]


def test_주어진_순서를_지킨다(tmp_path):
    """오래된 것부터 넣어야 중간에 끊겨도 앞쪽이 연속으로 남는다."""
    archives = [(n, tmp_path / f"{n}.tar") for n in ("d1", "d2", "d3")]
    assert [s.name for s in backfill.plan(archives, tmp_path)] == ["d1", "d2", "d3"]


def test_스테이징_경로는_덤프마다_나뉜다(tmp_path):
    steps = backfill.plan([("d1", tmp_path / "d1.tar")], tmp_path / "staging")
    assert steps[0].staging == tmp_path / "staging" / "d1"


def test_끝난_것이_없으면_전부_한다(tmp_path):
    archives = [(n, tmp_path / f"{n}.tar") for n in ("d1", "d2")]
    assert len(backfill.plan(archives, tmp_path)) == 2


# ── 용량 계산 ───────────────────────────────────────────────

def test_하위_폴더까지_더한다(tmp_path):
    (tmp_path / "안").mkdir()
    (tmp_path / "겉.bin").write_bytes(b"12345")
    (tmp_path / "안" / "속.bin").write_bytes(b"123")
    assert backfill.directory_size(tmp_path) == 8


def test_없는_경로는_0이다(tmp_path):
    assert backfill.directory_size(tmp_path / "없음") == 0


def test_단위를_읽기_좋게_바꾼다():
    assert backfill.format_bytes(512) == "512B"
    assert backfill.format_bytes(5 * 1024 ** 2) == "5MB"
    assert backfill.format_bytes(3 * 1024 ** 3) == "3.00GB"


# ── 컨테이너 이름 ────────────────────────────────────────────

def test_컨테이너_이름은_날짜로_구분된다():
    """뒤에서 자르면 모든 덤프가 'remental'로 끝나 같은 이름이 된다."""
    a = backfill.container_tag("listenbrainz-dump-2621-20260808-000003-incremental")
    b = backfill.container_tag("listenbrainz-dump-2622-20260809-000002-incremental")
    assert a == "20260808" and b == "20260809"
    assert a != b


def test_날짜가_없는_이름도_이름을_돌려준다():
    assert backfill.container_tag("이상한-이름") != ""


def test_적재가_남기는_기록_이름을_백필과_같은_상수로_만든다():
    """백필은 `MEASUREMENT_PREFIX`로 시작하는 기록을 보고 "이 덤프는 적재 끝"을 판단한다.
    적재 스크립트가 같은 글자를 따로 적어 두면, 한쪽만 바뀌었을 때 백필이 모든
    덤프를 다시 적재하거나 다 건너뛴다. 스크립트는 pyspark를 불러 호스트에서
    임포트되지 않으므로 원문으로 확인한다."""
    from pathlib import Path

    source = Path("scripts/pipeline/bronze_ingest.py").read_text(encoding="utf-8")
    assert "backfill.MEASUREMENT_PREFIX" in source
    assert 'write_measurement(f"bronze-' not in source
