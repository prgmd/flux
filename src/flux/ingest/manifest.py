import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

MANIFEST_NAME = "manifest.json"
VERSION = 1


class ManifestError(RuntimeError):
    """매니페스트를 읽거나 쓰지 못했을 때 발생한다."""


@dataclass(frozen=True)
class Entry:
    dump: str
    file: str
    path: str
    size: int
    downloaded_at: str
    checksums: dict = field(default_factory=dict)

    @property
    def key(self) -> str:
        return f"{self.dump}/{self.file}"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Manifest:
    """무엇을 언제 받았고 해시가 무엇이었는지를 기록한다.

    같은 덤프를 다시 받으라고 해도 여기를 보고 건너뛴다.
    S2의 멱등성을 판정하는 근거가 되므로 해시를 함께 남긴다.
    """

    def __init__(self, path):
        self.path = Path(path)
        self._entries: dict[str, Entry] = {}
        self._load()

    # ── 저장과 적재 ──────────────────────────────────────

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ManifestError(f"매니페스트가 깨졌다: {self.path}") from exc

        version = raw.get("version")
        if version != VERSION:
            raise ManifestError(
                f"모르는 매니페스트 버전 {version!r} (이 코드는 {VERSION}을 읽는다)"
            )
        for item in raw.get("entries", []):
            entry = Entry(**item)
            self._entries[entry.key] = entry

    def save(self) -> None:
        """임시 파일에 쓰고 이름을 바꾼다. 쓰다 죽어도 기존 기록이 깨지지 않는다."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": VERSION,
            "updated_at": _now(),
            "entries": [asdict(e) for e in self.sorted_entries()],
        }
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)

    # ── 조회 ────────────────────────────────────────────

    def sorted_entries(self) -> list[Entry]:
        return sorted(self._entries.values(), key=lambda e: e.key)

    def get(self, dump: str, file: str) -> Entry | None:
        return self._entries.get(f"{dump}/{file}")

    def has(self, dump: str, file: str) -> bool:
        return self.get(dump, file) is not None

    def is_complete(self, dump: str, file: str) -> bool:
        """기록이 있고, 그 파일이 실제로 그 크기로 존재하는가."""
        entry = self.get(dump, file)
        if entry is None:
            return False
        path = Path(entry.path)
        return path.exists() and path.stat().st_size == entry.size

    def __len__(self) -> int:
        return len(self._entries)

    def __contains__(self, key: str) -> bool:
        return key in self._entries

    # ── 기록 ────────────────────────────────────────────

    def record(self, dump: str, file: str, path, checksums: dict | None = None) -> Entry:
        path = Path(path)
        if not path.exists():
            raise ManifestError(f"기록하려는 파일이 없다: {path}")
        entry = Entry(
            dump=dump,
            file=file,
            path=str(path),
            size=path.stat().st_size,
            downloaded_at=_now(),
            checksums=dict(checksums or {}),
        )
        self._entries[entry.key] = entry
        self.save()
        return entry
