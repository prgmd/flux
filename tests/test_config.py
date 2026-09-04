import pytest

from flux.common import config


@pytest.fixture
def root(monkeypatch):
    def _set(value):
        monkeypatch.setenv(config.STORAGE_ROOT_ENV, value)
    return _set


def test_미설정이면_예외를_던진다(monkeypatch):
    monkeypatch.delenv(config.STORAGE_ROOT_ENV, raising=False)
    with pytest.raises(config.StorageConfigError):
        config.storage_root()


def test_빈_값도_예외를_던진다(root):
    root("   ")
    with pytest.raises(config.StorageConfigError):
        config.storage_root()


def test_스킴이_없는_상대경로는_거부한다(root):
    root("data/flux")
    with pytest.raises(config.StorageConfigError):
        config.storage_root()


@pytest.mark.parametrize(
    "given, expected",
    [
        ("file:///data/flux", "file:///data/flux"),
        ("file:///data/flux/", "file:///data/flux"),
        ("file:///data/flux///", "file:///data/flux"),
        ("s3a://bucket/flux", "s3a://bucket/flux"),
        ("s3a://bucket/flux/", "s3a://bucket/flux"),
        ("s3a://bucket", "s3a://bucket"),
    ],
)
def test_후행_슬래시를_정규화한다(root, given, expected):
    root(given)
    assert config.storage_root() == expected


@pytest.mark.parametrize(
    "given, expected",
    [
        ("/mnt/data/flux", "file:///mnt/data/flux"),
        (r"D:\flux-data", "file:///D:/flux-data"),
        ("D:/flux-data", "file:///D:/flux-data"),
    ],
)
def test_절대경로는_file_URI로_바꾼다(root, given, expected):
    root(given)
    assert config.storage_root() == expected


def test_스킴을_보존한다(root):
    root("s3a://bucket/flux")
    assert config.bronze("raw_listens") == "s3a://bucket/flux/bronze/raw_listens"


def test_계층별_경로를_만든다(root):
    root("file:///data/flux")
    assert config.bronze("raw_listens") == "file:///data/flux/bronze/raw_listens"
    assert config.silver("listens_normalized") == "file:///data/flux/silver/listens_normalized"
    assert config.gold("fact_listens") == "file:///data/flux/gold/fact_listens"
    assert config.dumps("listenbrainz", "full") == "file:///data/flux/dumps/listenbrainz/full"


def test_조각이_없으면_루트를_돌려준다(root):
    root("file:///data/flux")
    assert config.path() == "file:///data/flux"


@pytest.mark.parametrize("bad", ["", "  ", "/bronze", "bronze/", "..", "a/../b", "a//b"])
def test_잘못된_경로_조각을_거부한다(root, bad):
    root("file:///data/flux")
    with pytest.raises(config.StorageConfigError):
        config.path(bad)


def test_경로_탈출을_막는다(root):
    root("file:///data/flux")
    with pytest.raises(config.StorageConfigError):
        config.dumps("..", "..", "etc")


def test_로컬_여부를_구분한다(root):
    root("file:///data/flux")
    assert config.is_local() is True
    root("s3a://bucket/flux")
    assert config.is_local() is False


def test_환경변수를_바꾸면_즉시_반영된다(root):
    root("file:///data/flux")
    assert config.bronze("t") == "file:///data/flux/bronze/t"
    root("s3a://bucket/flux")
    assert config.bronze("t") == "s3a://bucket/flux/bronze/t"
