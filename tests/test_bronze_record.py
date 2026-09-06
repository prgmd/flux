import json
from datetime import datetime, timezone

import pytest

from flux.bronze import record, schema
from flux.bronze.record import Rejected, Row

AT = datetime(2026, 9, 6, 12, 0, 0, tzinfo=timezone.utc)
DUMP = "listenbrainz-dump-2651-20260904-000002-incremental"
FILE = "listens/2026/9.listens"

SAMPLE = {
    "user_id": 114258,
    "user_name": "Pfefan",
    "timestamp": 1788393521,
    "recording_msid": "379217d3-0c05-4cbe-9205-0fcb618246d4",
    "track_metadata": {
        "track_name": "Home To You",
        "artist_name": "The Neighborly",
        "additional_info": {"duration_ms": 202821, "submission_client": "Pano Scrobbler"},
    },
}


def build(line, index=0):
    return record.build(line, source_dump=DUMP, source_file=FILE, index=index, ingested_at=AT)


# ── 원본 보존 ───────────────────────────────────────────────

def test_원본_문자열을_그대로_보존한다():
    line = json.dumps(SAMPLE, ensure_ascii=False)
    row = build(line)
    assert isinstance(row, Row)
    assert row.raw_json == line, "한 글자도 바뀌면 안 된다"


def test_키_순서를_바꾸지_않는다():
    """json.dumps로 다시 만들면 순서가 정렬될 수 있다. 원문을 그대로 들고 있어야 한다."""
    line = '{"z_last": 1, "a_first": 2, "user_name": "x"}'
    row = build(line)
    assert row.raw_json == line


def test_공백만_잘라낸다():
    line = json.dumps(SAMPLE)
    row = build(f"  {line}  \n")
    assert row.raw_json == line


def test_유니코드를_이스케이프하지_않는다():
    line = '{"user_name": "김광민", "timestamp": 1}'
    row = build(line)
    assert "김광민" in row.raw_json


def test_바이트로_줘도_읽는다():
    line = json.dumps(SAMPLE, ensure_ascii=False)
    assert build(line.encode("utf-8")).raw_json == line


# ── 파생 컬럼 ───────────────────────────────────────────────

def test_timestamp를_listened_at으로_뽑는다():
    """실제 덤프의 필드 이름은 timestamp다. 명세는 listened_at으로 부른다."""
    row = build(json.dumps(SAMPLE))
    assert row.listened_at == 1788393521
    assert row.user_name == "Pfefan"


def test_listened_at이_있으면_그것을_쓴다():
    row = build('{"listened_at": 111, "timestamp": 222}')
    assert row.listened_at == 111


def test_문자열_숫자도_정수로_읽는다():
    assert build('{"timestamp": "1788393521"}').listened_at == 1788393521


@pytest.mark.parametrize("bad", ['{"timestamp": "어제"}', '{"timestamp": null}',
                                 '{"timestamp": true}', '{"timestamp": [1]}', "{}"])
def test_파생_실패는_격리_사유가_아니다(bad):
    """뽑기 실패는 None으로 두고 넘어간다. 원본은 raw_json에 남아 있다."""
    row = build(bad)
    assert isinstance(row, Row)
    assert row.listened_at is None
    assert row.raw_json == bad


def test_user_name이_문자열이_아니면_None():
    assert build('{"user_name": 123}').user_name is None


# ── 격리 ────────────────────────────────────────────────────

@pytest.mark.parametrize("bad", ["", "   ", "\n", "\t "])
def test_빈_줄은_격리한다(bad):
    rejected = build(bad)
    assert isinstance(rejected, Rejected)
    assert "빈 줄" in rejected.error


def test_깨진_JSON은_격리한다():
    rejected = build('{"user_name": "x"')
    assert isinstance(rejected, Rejected)
    assert "JSON 파싱 실패" in rejected.error
    assert rejected.raw_line == '{"user_name": "x"'


@pytest.mark.parametrize("bad", ["[1,2,3]", '"문자열"', "42", "null"])
def test_객체가_아니면_격리한다(bad):
    rejected = build(bad)
    assert isinstance(rejected, Rejected)
    assert "객체가 아니다" in rejected.error


def test_예외를_밖으로_던지지_않는다():
    """한 줄이 깨졌다고 적재 전체가 멈추면 안 된다."""
    for bad in ["", "{", "[]", "\x00\x01", "not json at all"]:
        assert isinstance(build(bad), (Row, Rejected))


def test_실패_사유가_길어도_잘라낸다():
    rejected = build("{" + "x" * 5000)
    assert len(rejected.error) <= record.MAX_ERROR_LEN


# ── 추적 정보 ───────────────────────────────────────────────

def test_출처를_모두_기록한다():
    row = build(json.dumps(SAMPLE), index=42)
    assert row._source_dump == DUMP
    assert row._source_file == FILE
    assert row._record_id == 42


def test_격리된_행에도_출처가_남는다():
    rejected = build("{", index=7)
    assert rejected._source_dump == DUMP
    assert rejected._record_id == 7


def test_파티션_값은_적재_날짜다():
    row = build(json.dumps(SAMPLE))
    assert row.ingested_date == AT.date()
    assert row.ingested_date.isoformat() == "2026-09-06"


def test_청취_시각이_과거여도_파티션은_적재일이다():
    """청취 시각은 21년에 걸쳐 흩어진다. 파티션이 그것을 따라가면 안 된다."""
    row = build('{"timestamp": 1108339087}')  # 2005년
    assert row.ingested_date == AT.date()


# ── 스키마와의 정합 ─────────────────────────────────────────

def test_행의_키가_스키마와_일치한다():
    row = build(json.dumps(SAMPLE))
    assert list(row.as_dict()) == schema.column_names()


def test_격리_행의_키가_격리_스키마와_일치한다():
    rejected = build("{")
    assert list(rejected.as_dict()) == schema.column_names(schema.QUARANTINE_COLUMNS)


def test_Kafka_컬럼은_스키마에_없다():
    """MVP에서 Kafka가 빠졌으므로 채울 수 없는 컬럼을 두지 않는다."""
    names = schema.column_names()
    assert "_kafka_partition" not in names
    assert "_kafka_offset" not in names
    assert "_source_file" in names and "_record_id" in names


def test_DDL에_NOT_NULL이_반영된다():
    ddl = schema.ddl()
    assert "raw_json string NOT NULL" in ddl
    assert "listened_at long" in ddl and "listened_at long NOT NULL" not in ddl
