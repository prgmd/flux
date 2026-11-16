"""Delta 표를 DuckDB로 읽어 SQL 파일이나 한 줄 질의를 실행한다. 읽기 전용이다. Spark와 Docker가 필요 없다.

    venv/Scripts/python scripts/investigations/query.py scripts/investigations/sql/top_artists.sql
    venv/Scripts/python scripts/investigations/query.py -e "select count(*) from {silver}"

SQL에서 `{bronze}`, `{monthly_user_recordings}`, `{monthly_coverage}` 등은 활성 릴리스를 읽는다.
`{silver}`, `{fact}`, `{dim_artist}` 등은 이전 일별 모델이다. 데이터 루트는 FLUX_STORAGE_ROOT 환경 변수나
저장소 루트의 .env에서 읽는다. 쓰는 질의(INSERT, CREATE, COPY 등)는 실행하지 않는다.
"""

import argparse
import os
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "src"))

import duckdb  # noqa: E402

from flux.common import active_release, config, queryrun  # noqa: E402


def resolve_root():
    if not os.environ.get(config.STORAGE_ROOT_ENV):
        env_file = REPO / ".env"
        value = queryrun.env_value(env_file.read_text(encoding="utf-8"), config.STORAGE_ROOT_ENV) \
            if env_file.exists() else None
        if not value:
            raise SystemExit(f"{config.STORAGE_ROOT_ENV}가 없다. 환경 변수나 .env에 데이터 루트를 적는다.")
        os.environ[config.STORAGE_ROOT_ENV] = value
    return config.local_root()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("file", nargs="?", help="실행할 .sql 파일")
    parser.add_argument("-e", "--execute", help="파일 대신 한 줄로 주는 질의")
    parser.add_argument("--rows", type=int, default=50, help="화면에 보일 최대 줄 수")
    parser.add_argument("--legacy", action="store_true", help="이전 일별 모델의 경로를 사용한다")
    args = parser.parse_args()
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    if bool(args.file) == bool(args.execute):
        parser.error("파일이나 -e 중 하나만 준다")
    text = args.execute if args.execute else Path(args.file).read_text(encoding="utf-8")
    if not queryrun.is_read_only(text):
        raise SystemExit("읽기 전용 질의(SELECT, WITH, FROM, DESCRIBE, SHOW, EXPLAIN, SUMMARIZE)만 실행한다.")

    root = resolve_root()
    release = None if args.legacy else active_release.active(root)
    queries = queryrun.statements(queryrun.expand(text, root, release=release))
    con = duckdb.connect()
    if any("delta_scan(" in statement.lower() for statement in queries):
        con.execute("LOAD delta")
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit='10GB'")
    con.execute("SET preserve_insertion_order=false")
    scratch = root / config.SCRATCH / "duckdb-tmp"
    scratch.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{scratch.as_posix()}'")

    started = time.time()
    for statement in queries:
        con.sql(statement).show(max_rows=args.rows, max_width=220)
    print(f"{time.time() - started:.1f}초")
    return 0


if __name__ == "__main__":
    sys.exit(main())
