"""급상승 마트의 최소 청취수 하한을 정하기 전에 잰다.

급상승은 "지난주보다 이번 주에 얼마나 늘었나"의 비율로 순위를 매긴다. 비율은 작은
수에서 요동친다. 지난주 1번, 이번 주 30번이면 30배인데, 그것은 급상승이 아니라 한
사람이 앨범 하나를 돌려 들은 것일 수 있다. 그래서 지난주 청취가 하한 이상인 아티스트만
본다. 그 하한을 얼마로 둘지가 질문이다.

재는 것은 셋이다.

    1. 아티스트의 주별 청취 수 분포. 하한이 아티스트 몇 %를 남기는가
    2. 하한마다 주별 상위 20의 모습. 지난주 청취의 중앙값, 청취한 사용자가 한 명뿐인
       아티스트가 몇이나 끼는가
    3. 하한을 한 단계 올렸을 때 상위 20이 얼마나 바뀌는가. 하한이 노이즈를 걸러 내는
       동안은 많이 바뀌고, 다 걸러 낸 뒤에는 덜 바뀐다

2의 사용자 수가 핵심이다. 청취가 많아도 한 사람이 만든 것이면 그 아티스트가 뜬 것이
아니다. 일별 마트에는 사용자가 없어서 팩트에서 센다.

주는 ISO 주(월요일 시작)다. 대상은 덤프 기간에 실시간으로 들어온 청취가 몰린 주다.
과거 주는 가져오기로 들어온 기록이라 "그 주에 뜬 것"을 뜻하지 않는다.

    FLUX_STORAGE_ROOT=<데이터 루트> venv/Scripts/python scripts/investigations/measure_rising_threshold.py
"""

import argparse
import sys

import duckdb

from flux.common import config, deltalog
from flux.gold import fact, mart

THRESHOLDS = (0, 5, 10, 20, 50, 100, 200)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-week", default="2026-08-10")
    parser.add_argument("--to-week", default="2026-08-31")
    parser.add_argument("--top", type=int, default=20)
    args = parser.parse_args()

    root = config.local_root()
    con = duckdb.connect()
    con.execute("SET TimeZone='UTC'")
    mart_files = deltalog.active_paths(root / config.GOLD / mart.DAILY_ARTIST)
    con.read_parquet([p.replace("\\", "/") for p in mart_files]).create_view("daily")
    fact_files = deltalog.files_in_partitions(
        root / config.GOLD / fact.TABLE_NAME, fact.PARTITION_COLUMN, ["2026-07", "2026-08", "2026-09"])
    con.read_parquet([f.replace("\\", "/") for f in fact_files]).create_view("recent")

    con.execute("""
        CREATE TABLE weekly AS
        SELECT artist_mbid, date_trunc('week', listened_date)::DATE AS week, sum(listens) AS listens
        FROM daily GROUP BY ALL
    """)
    con.execute("""
        CREATE TABLE listeners AS
        SELECT artist_mbid, date_trunc('week', listened_at)::DATE AS week,
               count(DISTINCT user_id) AS users
        FROM recent WHERE artist_mbid IS NOT NULL GROUP BY ALL
    """)
    con.execute(f"""
        CREATE TABLE pairs AS
        SELECT w.artist_mbid, w.week, w.listens, coalesce(p.listens, 0) AS prev,
               coalesce(u.users, 0) AS users
        FROM weekly w
        LEFT JOIN weekly p ON p.artist_mbid = w.artist_mbid AND p.week = w.week - INTERVAL 7 DAY
        LEFT JOIN listeners u ON u.artist_mbid = w.artist_mbid AND u.week = w.week
        WHERE w.week BETWEEN DATE '{args.from_week}' AND DATE '{args.to_week}'
    """)

    quantiles = con.execute("""
        SELECT quantile_cont(listens, [0.5, 0.9, 0.99]) FROM pairs
    """).fetchone()[0]
    print(f"주별 아티스트 청취 수  중앙값 {quantiles[0]:.0f}  90% {quantiles[1]:.0f}  99% {quantiles[2]:.0f}")
    print()

    weeks = [r[0] for r in con.execute("SELECT DISTINCT week FROM pairs ORDER BY week").fetchall()]
    results = []
    previous = None
    print(f"{'하한':>5} {'남는 비율':>8} {'상위 지난주 중앙':>14} {'1인 아티스트':>11} {'앞 하한과 겹침':>12}")
    for threshold in THRESHOLDS:
        kept = con.execute(
            "SELECT avg(CASE WHEN prev >= ? THEN 1 ELSE 0 END) FROM pairs WHERE prev > 0",
            [max(threshold, 1)]).fetchone()[0]
        top = con.execute(f"""
            SELECT week, artist_mbid, listens, prev, users FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY week ORDER BY listens::DOUBLE / prev DESC, listens DESC, artist_mbid
                ) AS r
                FROM pairs WHERE prev >= ? AND prev > 0
            ) WHERE r <= {args.top}
        """, [threshold]).fetchall()
        chosen = {(w, a) for w, a, *_ in top}
        median_prev = sorted(p for *_, p, _ in top)[len(top) // 2] if top else 0
        single = sum(1 for *_, u in top if u <= 1)
        overlap = (len(chosen & previous) / len(chosen)) if previous and chosen else None
        print(f"{threshold:>5} {kept * 100:7.1f}% {median_prev:>14,} "
              f"{single:>5}/{len(top):<5} {'-' if overlap is None else f'{overlap * 100:.0f}%':>12}")
        results.append({"threshold": threshold, "kept_share": round(kept, 4),
                        "top_median_prev": median_prev, "single_listener": single,
                        "top_rows": len(top), "overlap_with_lower": overlap})
        previous = chosen

    config.write_measurement("rising-threshold", {
        "weeks": [str(w) for w in weeks],
        "weekly_listens_quantiles": quantiles,
        "thresholds": results,
    })
    return 0


if __name__ == "__main__":
    sys.exit(main())
