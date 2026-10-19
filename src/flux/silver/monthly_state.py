import os
from contextlib import contextmanager
from pathlib import Path


def separate(source, target):
    source, target = Path(source).resolve(), Path(target).resolve()
    if source == target or source in target.parents or target in source.parents:
        raise ValueError("원본과 월별 Silver 경로는 분리해야 한다")


@contextmanager
def exclusive(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    lock = root / ".writer.lock"
    fd = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    try:
        os.write(fd, str(os.getpid()).encode())
        yield
    finally:
        os.close(fd)
        lock.unlink()
