"""질의 실행기가 쓰는 순수 판단. DuckDB를 부르지 않는다.

SQL 안의 `{silver}` 같은 이름을 Delta 폴더 읽기 식으로 바꾸고, 읽기 전용 질의만 통과시킨다.
실제 실행은 scripts/investigations/query.py가 한다.
"""

import re

from flux.bronze import schema as bronze_schema
from flux.common import config
from flux.gold import fact, mart, scd2
from flux.silver import schema as silver_schema

TABLES = {
    "bronze": (config.BRONZE, bronze_schema.TABLE_NAME),
    "bronze_invalid": (config.BRONZE, bronze_schema.QUARANTINE_TABLE_NAME),
    "silver": (config.SILVER, silver_schema.TABLE_NAME),
    "silver_invalid": (config.SILVER, silver_schema.QUARANTINE_TABLE_NAME),
    "user_recording_counts": (config.SILVER, "user_recording_counts"),
    "fact": (config.GOLD, fact.TABLE_NAME),
    "dim_artist": (config.GOLD, scd2.TABLE_NAME),
    "mart_daily": (config.GOLD, mart.DAILY_ARTIST),
}

ALLOWED_FIRST_WORDS = frozenset({"SELECT", "WITH", "FROM", "DESCRIBE", "SHOW", "EXPLAIN", "SUMMARIZE"})

_PLACEHOLDER = re.compile(r"\{(" + "|".join(TABLES) + r")\}")
_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.S)


def expand(sql, root):
    base = str(root).replace("\\", "/").rstrip("/")

    def table(match):
        layer, name = TABLES[match.group(1)]
        return f"delta_scan('{base}/{layer}/{name}')"

    return _PLACEHOLDER.sub(table, sql)


def statements(sql):
    cleaned = _BLOCK_COMMENT.sub(" ", _LINE_COMMENT.sub(" ", sql))
    return [part.strip() for part in cleaned.split(";") if part.strip()]


def is_read_only(sql):
    parts = statements(sql)
    if not parts:
        return False
    return all(part.split(None, 1)[0].upper().lstrip("(") in ALLOWED_FIRST_WORDS for part in parts)


def env_value(text, key):
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        name, _, value = stripped.partition("=")
        if name.strip() == key:
            return value.strip().strip('"').strip("'")
    return None
