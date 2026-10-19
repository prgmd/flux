import pytest

from flux.silver.monthly_state import exclusive, separate


def test_중첩된_입출력_경로를_거부한다(tmp_path):
    with pytest.raises(ValueError):
        separate(tmp_path, tmp_path / "silver")
    separate(tmp_path / "bronze", tmp_path / "silver")


def test_동시_작성자를_막고_예외_후_잠금을_해제한다(tmp_path):
    root = tmp_path / "monthly"
    with pytest.raises(RuntimeError, match="실패"):
        with exclusive(root):
            with pytest.raises(FileExistsError):
                with exclusive(root):
                    pass
            raise RuntimeError("실패")
    with exclusive(root):
        assert (root / ".writer.lock").exists()
    assert not (root / ".writer.lock").exists()
