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
    assert config.scratch("drill", "dim_artist") == "file:///data/flux/scratch/drill/dim_artist"


def test_임시_사본은_계층_디렉터리_밖에_있다(root):
    """드릴이 끝나면 사본 디렉터리를 통째로 지운다. 그 안에 운영 테이블이 있으면
    함께 지워진다."""
    root("file:///data/flux")
    work = config.scratch("drill")
    for layer in (config.BRONZE, config.SILVER, config.GOLD):
        assert not work.startswith(config.path(layer) + "/")


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


# ── 로컬 파일 경로로 되돌리기 ────────────────────────────────

def test_유닉스_절대경로는_절대경로로_남는다(root):
    """file:///data 에서 앞 슬래시를 떼면 상대 경로가 된다.

    7개 스크립트에 복사돼 있던 구현이 이 경우에 틀렸다.
    윈도우에서만 돌려 봐서 드러나지 않았다.
    """
    root("file:///data/flux")
    assert config.local_root().as_posix() == "/data/flux"


def test_윈도우_드라이브는_앞_슬래시를_뗀다(root):
    root("file:///C:/Users/flux-data")
    assert config.local_root().as_posix() == "C:/Users/flux-data"


def test_드라이브_문자_하나만_있어도_뗀다(root):
    root("file:///D:")
    assert config.local_root().as_posix() == "D:"


def test_드라이브처럼_생긴_폴더는_떼지_않는다(root):
    """/data 는 드라이브가 아니다. 한 글자 폴더와 구분해야 한다."""
    root("file:///d/flux")
    assert config.local_root().as_posix() == "/d/flux"


def test_로컬이_아니면_거부한다(root):
    root("s3a://bucket/flux")
    with pytest.raises(config.StorageConfigError):
        config.local_root()


def test_Spark_경로는_스킴을_유지한다(root):
    """Spark에 넘기는 경로는 local_root가 아니라 bronze/silver/gold를 쓴다."""
    root("s3a://bucket/flux")
    assert config.bronze("raw_listens") == "s3a://bucket/flux/bronze/raw_listens"
