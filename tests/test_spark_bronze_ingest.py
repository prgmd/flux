"""Bronze 적재가 record.build와 같은 것을 격리하는가.

같은 줄을 두 구현에 넣고 판정이 같은지 본다. 한쪽만 고치면 여기서 걸린다.

record.build는 호스트에서 돌고, ingest는 Spark 표현이다. 규칙이 갈리면
**테스트가 보장하는 것과 실제로 도는 것이 다르다.**
"""

import pytest

# -m 필터는 수집이 끝난 뒤에 걸린다. 그래서 여기서 먼저 막아야 한다.
# 이 아래 import가 pyspark를 부르므로, 없는 환경에서는 수집 자체가 실패한다.
pytest.importorskip("pyspark", reason="Spark 없이 도는 환경이다")
pytest.importorskip("delta", reason="delta-spark가 없다")

from flux.bronze import ingest, record, schema

pytestmark = pytest.mark.spark

DUMP = "listenbrainz-dump-2651-20260904-000002-incremental"
FILE = "0.listens"

# 성한 줄 둘, 격리될 줄 넷. 사유 세 가지를 다 덮는다.
LINES = [
    '{"user_name": "alice", "listened_at": 1700000000, '
    '"track_metadata": {"track_name": "곡", "artist_name": "가수"}}',
    '{"user_name": "bob", "timestamp": 1700000001, "track_metadata": {}}',
    "",
    "   ",
    '{"user_name": "carol"',
    "[1,2,3]",
]


@pytest.fixture
def ingested(spark, tmp_path):
    """줄들을 파일로 쓰고 적재 데이터프레임을 만든다."""
    source = tmp_path / FILE
    source.write_text("\n".join(LINES), encoding="utf-8")

    df = ingest.build_dataframe(
        spark, source.as_posix(), source_dump=DUMP, source_file=FILE)
    return ingest.split(df)


def _reasons(rejected) -> list:
    return sorted(row["error"] for row in rejected.select("error").collect())


def test_원본_줄_수가_유효와_격리의_합과_같다(ingested):
    """flux.bronze.verify의 대조 등식이 보는 것이다. 여기서 미리 확인한다."""
    valid, rejected = ingested
    assert valid.count() + rejected.count() == len(LINES)


def test_성한_줄만_통과한다(ingested):
    valid, _ = ingested
    assert valid.count() == 2
    assert sorted(row["user_name"] for row in valid.collect()) == ["alice", "bob"]


def test_격리_사유가_record_build와_같다(ingested):
    """두 구현에 같은 줄을 넣어 사유를 대조한다.

    record.build는 사유에 상세를 덧붙이므로(`JSON 파싱 실패: ...`)
    앞부분만 본다. 앞부분이 Reason의 값이다.
    """
    _, rejected = ingested

    expected = []
    for index, line in enumerate(LINES):
        verdict = record.build(line, source_dump=DUMP, source_file=FILE,
                               index=index)
        if isinstance(verdict, record.Rejected):
            expected.append(verdict.error.split(":")[0])

    assert _reasons(rejected) == sorted(expected)


def test_listened_at을_두_이름에서_찾는다(ingested):
    """덤프의 필드 이름은 timestamp지만 명세는 listened_at이라고 부른다.
    record.build도 두 이름을 본다."""
    valid, _ = ingested
    found = {row["user_name"]: row["listened_at"] for row in valid.collect()}
    assert found == {"alice": 1700000000, "bob": 1700000001}


def test_raw_json을_변형하지_않는다(ingested):
    """파싱해서 다시 직렬화하면 키 순서와 숫자 표기가 바뀐다.
    Bronze의 원칙은 원본을 문자열 그대로 담는 것이다."""
    valid, _ = ingested
    stored = {row["user_name"]: row["raw_json"] for row in valid.collect()}
    assert stored["alice"] == LINES[0]
    assert stored["bob"] == LINES[1]


def test_격리_행이_원본_줄을_그대로_들고_간다(ingested):
    """판단이 틀렸을 때 되돌릴 수 있어야 한다."""
    _, rejected = ingested
    kept = {row["raw_line"] for row in rejected.select("raw_line").collect()}
    assert "[1,2,3]" in kept
    assert '{"user_name": "carol"' in kept


def test_두_스키마의_컬럼과_순서가_선언과_같다(ingested):
    valid, rejected = ingested
    assert valid.columns == schema.column_names()
    assert rejected.columns == schema.column_names(schema.QUARANTINE_COLUMNS)


def test_record_id가_덤프_안에서_유일하다(ingested):
    """monotonically_increasing_id()가 주는 것은 잡 하나 안에서의 유일함이다.
    덤프를 건너면 겹친다 — 그래서 청취 키가 (_source_dump, _record_id) 쌍이다."""
    valid, rejected = ingested
    ids = [row["_record_id"] for row in valid.select("_record_id").collect()]
    ids += [row["_record_id"] for row in rejected.select("_record_id").collect()]
    assert len(ids) == len(set(ids))
