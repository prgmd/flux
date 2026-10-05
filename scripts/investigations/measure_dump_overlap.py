"""증분 Silver를 어떻게 만들지 정하기 전에 잰다. 덤프 하나가 Silver의 어디를 건드리는가.

계획은 "덤프 하나가 오면 그 덤프가 건드린 달만 다시 만든다"였다. 같은 청취의 사본은
청취 시각이 같으므로 같은 달에 있고, 그래서 달 단위로 다시 만들어도 결과는 정확하다.
문제는 그 달이 몇 개냐다. 그리고 달 대신 키 단위로 고친다면, 고쳐 써야 할 파일이
몇 개냐다.

재는 것은 둘이다.

    1. 덤프마다 청취가 퍼진 달의 수와 최근 두 달의 비율
    2. 덤프마다 앞 덤프에 이미 있던 키(중복 제거 키)의 수, 그 키를 가진 사용자 수,
       그 키가 든 지금 Silver 파일의 수, (달, 사용자) 쌍의 수

2의 마지막 둘이 파일 배치의 비교다. 지금처럼 한 달을 키 해시로 여러 파일에 흩으면
건드릴 파일이 "그 키가 든 파일 수"이고, 한 사용자의 한 달 치를 한 파일에 모으면
"(달, 사용자) 쌍의 수"를 넘지 않는다.

Spark가 아니라 DuckDB로 잰다. 조사는 잡이 아니다. 열 몇 개만 읽으면 되는 질문이라
컨테이너 없이 호스트에서 바로 돈다.

**DuckDB 세션 시간대를 UTC로 둔다.** 정하지 않으면 노트북 시간대(서울)를 따른다.
Silver의 청취 시각을 초로 바꿀 때 9시간이 어긋나 Bronze와의 조인이 거의 다 빠졌다.
Spark 세션을 UTC로 못 박은 것과 같은 이유다(flux.common.spark.TIME_ZONE).

`recording_msid`는 Bronze에서 `raw_json` 안에만 있다. 1억 5천만 줄을 전부 파싱하면
무겁다. (사용자, 청취 시각)이 두 번 이상 나오는 줄만 골라 그 줄에서만 꺼낸다.
키가 같으려면 이 둘이 먼저 같아야 하므로 빠지는 중복은 없다.

    FLUX_STORAGE_ROOT=<데이터 루트> venv/Scripts/python scripts/investigations/measure_dump_overlap.py
"""

import sys
import time

import duckdb

from flux.bronze import schema as bronze_schema
from flux.common import config, deltalog
from flux.silver import schema as silver_schema


def main() -> int:
    root = config.local_root()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    con.execute("SET memory_limit='12GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{(root / config.SCRATCH / 'duckdb-tmp').as_posix()}'")

    bronze_files = deltalog.active_paths(root / config.BRONZE / bronze_schema.TABLE_NAME)
    silver_files = deltalog.active_paths(root / config.SILVER / silver_schema.TABLE_NAME)
    con.read_parquet([p.replace("\\", "/") for p in bronze_files]).create_view("bronze")
    con.read_parquet([p.replace("\\", "/") for p in silver_files],
                     hive_partitioning=True, filename=True).create_view("silver")
    started = time.time()

    spread = con.execute("""
        SELECT _source_dump, count(*),
               count(DISTINCT strftime(to_timestamp(listened_at), '%Y-%m')),
               avg(CASE WHEN listened_at >= epoch(TIMESTAMP '2026-08-01') THEN 1 ELSE 0 END)
        FROM bronze WHERE listened_at IS NOT NULL
        GROUP BY ALL
    """).fetchall()
    rows = {d: (n, months, recent) for d, n, months, recent in spread}

    con.execute("""
        CREATE TABLE pairs AS
        SELECT user_name u, listened_at t FROM bronze GROUP BY ALL HAVING count(*) > 1
    """)
    con.execute("""
        CREATE TABLE candidates AS
        SELECT b._source_dump d, b.user_name u, b.listened_at t,
               json_extract_string(b.raw_json, '$.recording_msid') msid
        FROM bronze b SEMI JOIN pairs p ON b.user_name = p.u AND b.listened_at = p.t
    """)
    con.execute("""
        CREATE TABLE seen AS
        WITH first AS (SELECT u, t, msid, min(d) fd FROM candidates GROUP BY ALL)
        SELECT c.* FROM candidates c JOIN first f USING (u, t, msid) WHERE f.fd < c.d
    """)
    con.execute("""
        CREATE TABLE located AS
        SELECT user_name u, epoch(listened_at)::BIGINT t, recording_msid msid,
               filename f, listened_month m
        FROM silver SEMI JOIN pairs p
             ON user_name = p.u AND epoch(listened_at)::BIGINT = p.t
    """)
    overlap = con.execute("""
        SELECT s.d, count(*), count(DISTINCT s.u), count(DISTINCT l.f),
               count(DISTINCT l.m), count(DISTINCT (l.m, s.u))
        FROM seen s JOIN located l USING (u, t, msid) GROUP BY s.d
    """).fetchall()
    seen = {d: (n, users, files, months, pairs) for d, n, users, files, months, pairs in overlap}
    in_dump = con.execute(
        "SELECT count(*) - count(DISTINCT (d, u, t, msid)) FROM candidates").fetchone()[0]
    elapsed = time.time() - started

    total_files = len(silver_files)
    print(f"Silver 파일 {total_files:,}개, 한 덤프 안 키 중복 {in_dump:,}, {elapsed:.0f}초")
    print()
    print(f"{'덤프':14} {'행':>10} {'달':>4} {'최근':>6}   {'앞 덤프 키':>10} {'사용자':>5} "
          f"{'지금 배치 파일':>12} {'(달,사용자)':>10}")
    per_dump = []
    for d in sorted(rows):
        n, months, recent = rows[d]
        hit, users, files, _, pairs = seen.get(d, (0, 0, 0, 0, 0))
        print(f"{d[17:31]:14} {n:>10,} {months:>4} {recent * 100:5.1f}%   {hit:>10,} {users:>5} "
              f"{files:>12,} {pairs:>10,}")
        per_dump.append({"dump": d, "rows": n, "months": months, "recent_share": round(recent, 4),
                         "seen_before": hit, "users": users, "silver_files_now": files,
                         "month_user_pairs": pairs})

    config.write_measurement("dump-overlap", {
        "silver_files": total_files,
        "in_dump_key_duplicates": in_dump,
        "elapsed_seconds": round(elapsed, 1),
        "dumps": per_dump,
    })
    return 0


if __name__ == "__main__":
    sys.exit(main())
