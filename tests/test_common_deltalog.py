"""Spark 없이 Delta 테이블의 활성 파일을 가려내는 규칙.

폴더를 glob하면 지워진 파일과 고아 파일을 같이 읽는다. 로그가 진실이다.
"""

import json

import pytest

from flux.common import deltalog


def table(tmp_path, *commits, orphans=()):
    """커밋 목록으로 가짜 Delta 테이블을 만든다.

    `commits`는 버전마다의 동작 목록이고, `orphans`는 로그에 없는 채로
    폴더에만 있는 파일이다. 강제 종료가 남기는 것이 이것이다.
    """
    log = tmp_path / deltalog.LOG_DIR
    log.mkdir(parents=True, exist_ok=True)
    for version, acts in enumerate(commits):
        lines = [json.dumps(a) for a in acts]
        (log / f"{version:020d}.json").write_text("\n".join(lines),
                                                  encoding="utf-8")
    for name in orphans:
        (tmp_path / name).write_text("", encoding="utf-8")
    return tmp_path


def add(path, **parts):
    return {"add": {"path": path, "partitionValues": parts, "size": 1}}


def remove(path):
    return {"remove": {"path": path, "dataChange": True}}


# ── 무엇이 살아 있는가 ──────────────────────────────────────

def test_추가된_파일이_살아_있다(tmp_path):
    t = table(tmp_path, [add("a.parquet"), add("b.parquet")])
    assert deltalog.active_files(t) == ["a.parquet", "b.parquet"]


def test_지워진_파일은_빠진다(tmp_path):
    """덮어쓰기가 옛 파일을 remove한다. 디스크에는 남아 있다."""
    t = table(tmp_path,
              [add("a.parquet")],
              [remove("a.parquet"), add("b.parquet")])
    assert deltalog.active_files(t) == ["b.parquet"]


def test_로그에_없는_고아_파일은_안_읽는다(tmp_path):
    """강제 종료가 남긴 parquet이다. 실제로 Bronze에서 44개가 나왔고,
    그것들이 읽히지 않는 것이 Delta ACID의 증거였다."""
    t = table(tmp_path, [add("a.parquet")],
              orphans=["orphan1.parquet", "orphan2.parquet"])
    assert deltalog.active_files(t) == ["a.parquet"]


def test_커밋을_버전_순서로_적용한다(tmp_path):
    """순서가 뒤집히면 지워진 것이 살아나거나 그 반대가 된다."""
    t = table(tmp_path,
              [add("a.parquet")],
              [remove("a.parquet")],
              [add("a.parquet")])
    assert deltalog.active_files(t) == ["a.parquet"]


def test_동작이_아닌_줄은_무시한다(tmp_path):
    """커밋에는 metaData, protocol, commitInfo도 들어 있다."""
    t = table(tmp_path, [
        {"protocol": {"minReaderVersion": 1}},
        {"metaData": {"id": "x"}},
        add("a.parquet"),
        {"commitInfo": {"operation": "WRITE"}},
    ])
    assert deltalog.active_files(t) == ["a.parquet"]


def test_깨진_줄을_건너뛴다(tmp_path):
    log = tmp_path / deltalog.LOG_DIR
    log.mkdir(parents=True)
    (log / f"{0:020d}.json").write_text(
        json.dumps(add("a.parquet")) + '\n{"add": {"pa\n'
        + json.dumps(add("b.parquet")), encoding="utf-8")
    assert deltalog.active_files(tmp_path) == ["a.parquet", "b.parquet"]


def test_로그가_없으면_거부한다(tmp_path):
    with pytest.raises(FileNotFoundError):
        deltalog.active_files(tmp_path)


def test_체크포인트_버전을_읽는다(tmp_path):
    t = table(tmp_path, [add("a.parquet")])
    (t / deltalog.LOG_DIR / deltalog.LAST_CHECKPOINT).write_text(
        json.dumps({"version": 10, "size": 3}), encoding="utf-8")
    assert deltalog.checkpoint_version(t / deltalog.LOG_DIR) == 10


def test_체크포인트가_없으면_None이다(tmp_path):
    t = table(tmp_path, [add("a.parquet")])
    assert deltalog.checkpoint_version(t / deltalog.LOG_DIR) is None


def test_가리키는_체크포인트_파일이_없으면_멈춘다(tmp_path):
    """조용히 JSON만 읽으면 오라클이 덜 읽고서 Spark가 틀렸다고 보고한다."""
    t = table(tmp_path, [add("a.parquet")])
    (t / deltalog.LOG_DIR / deltalog.LAST_CHECKPOINT).write_text(
        json.dumps({"version": 10}), encoding="utf-8")
    with pytest.raises(deltalog.CheckpointMissing):
        deltalog.active_files(t)


def test_체크포인트_표시가_깨졌으면_없는_것으로_본다(tmp_path):
    t = table(tmp_path, [add("a.parquet")])
    (t / deltalog.LOG_DIR / deltalog.LAST_CHECKPOINT).write_text(
        "깨진 내용", encoding="utf-8")
    assert deltalog.active_files(t) == ["a.parquet"]


def test_체크포인트_뒤의_커밋만_적용한다(tmp_path):
    """체크포인트가 그 앞을 이미 담고 있다. 다시 적용하면 지워진 것이 살아난다."""
    t = table(tmp_path, [add("a.parquet")], [remove("a.parquet")])
    assert [p.name for p in deltalog.commits(t, after=0)] == [f"{1:020d}.json"]


# ── 파티션 ──────────────────────────────────────────────────

def test_파일마다_파티션_값을_돌려준다(tmp_path):
    t = table(tmp_path, [add("m=2026-09/a.parquet", listened_month="2026-09")])
    assert deltalog.partition_values(t) == {
        "m=2026-09/a.parquet": {"listened_month": "2026-09"}}


def test_파티션으로_읽을_파일을_줄인다(tmp_path):
    """한 주를 재려고 260개 파티션을 다 읽을 이유가 없다."""
    t = table(tmp_path, [
        add("a.parquet", listened_month="2026-08"),
        add("b.parquet", listened_month="2026-09"),
        add("c.parquet", listened_month="2005-01"),
    ])
    picked = deltalog.files_in_partitions(t, "listened_month", ["2026-09"])
    assert [p.split("\\")[-1].split("/")[-1] for p in picked] == ["b.parquet"]


def test_여러_파티션을_한꺼번에_고른다(tmp_path):
    t = table(tmp_path, [
        add("a.parquet", listened_month="2026-08"),
        add("b.parquet", listened_month="2026-09"),
    ])
    assert len(deltalog.files_in_partitions(
        t, "listened_month", ["2026-08", "2026-09"])) == 2


def test_지워진_파일은_파티션에서도_빠진다(tmp_path):
    t = table(tmp_path,
              [add("a.parquet", listened_month="2026-09")],
              [remove("a.parquet")])
    assert deltalog.files_in_partitions(t, "listened_month", ["2026-09"]) == []


def test_절대_경로를_돌려준다(tmp_path):
    """다른 엔진에 그대로 넘길 수 있어야 한다."""
    t = table(tmp_path, [add("a.parquet")])
    assert deltalog.active_paths(t)[0].endswith("a.parquet")
    assert str(tmp_path) in deltalog.active_paths(t)[0]
