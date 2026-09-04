import json

import pytest

from flux.ingest.manifest import Manifest, ManifestError

DUMP = "listenbrainz-dump-2651-20260904-000002-incremental"
FILE = "listenbrainz-listens-dump-2651-20260904-000002-incremental.tar.zst"


@pytest.fixture
def data_file(tmp_path):
    path = tmp_path / "data" / FILE
    path.parent.mkdir(parents=True)
    path.write_bytes(b"payload" * 100)
    return path


@pytest.fixture
def manifest(tmp_path):
    return Manifest(tmp_path / "manifest.json")


# ── 기록과 조회 ──────────────────────────────────────────────

def test_처음에는_비어_있다(manifest):
    assert len(manifest) == 0
    assert not manifest.has(DUMP, FILE)


def test_기록하면_조회된다(manifest, data_file):
    manifest.record(DUMP, FILE, data_file, {"sha256": "abc"})
    assert manifest.has(DUMP, FILE)
    entry = manifest.get(DUMP, FILE)
    assert entry.size == data_file.stat().st_size
    assert entry.checksums == {"sha256": "abc"}
    assert entry.downloaded_at.endswith("+00:00"), "시각은 UTC로 남긴다"


def test_없는_파일은_기록하지_않는다(manifest, tmp_path):
    with pytest.raises(ManifestError):
        manifest.record(DUMP, FILE, tmp_path / "없는파일.zst")


def test_다른_덤프는_섞이지_않는다(manifest, data_file):
    manifest.record(DUMP, FILE, data_file)
    assert not manifest.has("다른-덤프", FILE)
    assert not manifest.has(DUMP, "다른-파일.zst")


# ── 파일로 유지된다 ──────────────────────────────────────────

def test_기록하면_즉시_저장된다(manifest, data_file):
    manifest.record(DUMP, FILE, data_file)
    assert manifest.path.exists()
    raw = json.loads(manifest.path.read_text(encoding="utf-8"))
    assert raw["version"] == 1
    assert len(raw["entries"]) == 1


def test_다시_열면_기록이_남아_있다(tmp_path, data_file):
    path = tmp_path / "manifest.json"
    Manifest(path).record(DUMP, FILE, data_file, {"md5": "xyz"})

    reopened = Manifest(path)
    assert reopened.has(DUMP, FILE)
    assert reopened.get(DUMP, FILE).checksums == {"md5": "xyz"}


def test_임시파일을_남기지_않는다(manifest, data_file):
    manifest.record(DUMP, FILE, data_file)
    leftovers = [p.name for p in manifest.path.parent.iterdir() if p.name.endswith(".tmp")]
    assert leftovers == []


def test_깨진_매니페스트는_실패시킨다(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text("{망가진 json", encoding="utf-8")
    with pytest.raises(ManifestError):
        Manifest(path)


def test_모르는_버전은_실패시킨다(tmp_path):
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"version": 99, "entries": []}), encoding="utf-8")
    with pytest.raises(ManifestError) as exc:
        Manifest(path)
    assert "99" in str(exc.value)


# ── 건너뛰기 판정 ────────────────────────────────────────────

def test_기록이_있고_파일도_있으면_완료로_본다(manifest, data_file):
    manifest.record(DUMP, FILE, data_file)
    assert manifest.is_complete(DUMP, FILE)


def test_기록이_없으면_완료가_아니다(manifest):
    assert not manifest.is_complete(DUMP, FILE)


def test_파일이_사라졌으면_완료가_아니다(manifest, data_file):
    """기록만 믿지 않는다. 파일이 지워졌으면 다시 받아야 한다."""
    manifest.record(DUMP, FILE, data_file)
    data_file.unlink()
    assert not manifest.is_complete(DUMP, FILE)


def test_크기가_달라졌으면_완료가_아니다(manifest, data_file):
    """받다 만 파일로 바뀌었거나 손상된 경우를 잡는다."""
    manifest.record(DUMP, FILE, data_file)
    data_file.write_bytes(b"short")
    assert not manifest.is_complete(DUMP, FILE)


# ── 여러 건 ─────────────────────────────────────────────────

def test_여러_건을_이름순으로_저장한다(manifest, tmp_path):
    for name in ["c.zst", "a.zst", "b.zst"]:
        f = tmp_path / "data" / name
        f.parent.mkdir(parents=True, exist_ok=True)
        f.write_bytes(b"x")
        manifest.record(DUMP, name, f)

    raw = json.loads(manifest.path.read_text(encoding="utf-8"))
    files = [e["file"] for e in raw["entries"]]
    assert files == sorted(files), "정렬해 두면 변경 내역을 읽기 쉽다"


def test_같은_파일을_다시_기록하면_덮어쓴다(manifest, data_file):
    manifest.record(DUMP, FILE, data_file, {"sha256": "old"})
    manifest.record(DUMP, FILE, data_file, {"sha256": "new"})
    assert len(manifest) == 1
    assert manifest.get(DUMP, FILE).checksums == {"sha256": "new"}
