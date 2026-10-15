"""질의 실행기의 순수 판단: 이름 바꾸기, 읽기 전용 검사, .env 읽기."""

import pytest

from flux.common import queryrun


def test_이름을_Delta_읽기_식으로_바꾼다():
    sql = queryrun.expand("select * from {silver}", "C:/data")
    assert sql == "select * from delta_scan('C:/data/silver/listens_normalized')"


def test_모든_등록_이름을_바꾼다():
    for name in queryrun.TABLES:
        assert "{" not in queryrun.expand("{" + name + "}", "C:/data")


def test_역슬래시_경로도_슬래시로_바꾼다():
    assert "C:/a/b/silver" in queryrun.expand("{silver}", "C:\\a\\b")


def test_끝_슬래시는_겹치지_않는다():
    assert "data//" not in queryrun.expand("{fact}", "C:/data/")


def test_등록되지_않은_중괄호는_건드리지_않는다():
    sql = "select {'a': 1} as s, '{x}' as t"
    assert queryrun.expand(sql, "C:/data") == sql


@pytest.mark.parametrize("sql", [
    "select 1",
    "  SELECT 1;",
    "with a as (select 1) select * from a",
    "from {silver} limit 1",
    "describe select 1",
    "-- 설명\nselect 1",
    "/* 설명 */ select 1",
    "(select 1) union (select 2)",
    "select 1; select 2",
])
def test_읽기_전용_질의는_통과한다(sql):
    assert queryrun.is_read_only(sql)


@pytest.mark.parametrize("sql", [
    "",
    "   ",
    "-- 주석뿐",
    "drop table t",
    "create table t as select 1",
    "insert into t values (1)",
    "update t set a = 1",
    "delete from t",
    "copy (select 1) to 'x.csv'",
    "attach 'other.db'",
    "select 1; drop table t",
    "-- select\ndrop table t",
    "install httpfs",
])
def test_쓰는_질의는_막는다(sql):
    assert not queryrun.is_read_only(sql)


def test_env에서_값을_읽는다():
    text = "# 주석\nFLUX_STORAGE_ROOT=C:/data\nOTHER=1\n"
    assert queryrun.env_value(text, "FLUX_STORAGE_ROOT") == "C:/data"


def test_env의_따옴표와_공백을_정리한다():
    assert queryrun.env_value('  KEY = "C:/a b"  \n', "KEY") == "C:/a b"


def test_env에_없는_키는_None이다():
    assert queryrun.env_value("A=1\n", "B") is None


def test_주석_처리된_env_줄은_무시한다():
    assert queryrun.env_value("# KEY=1\n", "KEY") is None
