"""집계 마트를 **Spark가 아닌 엔진**으로 다시 세어 대조한다.

명세 S4의 게이트다.

    정합성 오라클. 1주일치 샘플을 Spark가 아닌 다른 엔진으로 단순 집계해
    결과를 대조한다. **Spark 쿼리가 틀렸는지를 Spark로 검증할 수는 없다.**

마지막 문장이 핵심이다. 같은 엔진으로 두 번 세면 같은 버그를 두 번 통과한다.
윈도우 함수를 잘못 쓴 것도, 조인 조건을 빠뜨린 것도 양쪽에 똑같이 들어간다.

그래서 DuckDB를 쓴다. 구현도 언어도 실행 모델도 Spark와 겹치지 않는다.
**같은 답이 나오면 두 엔진이 동시에 같은 방식으로 틀렸을 때뿐**이고, 그럴
확률은 한 엔진이 틀릴 확률보다 훨씬 낮다.

## 왜 한 주인가

전수를 DuckDB로 세면 오라클이 본 잡만큼 무거워진다. 오라클은 **싸고 독립적**
이어야 계속 돌릴 수 있다. 한 주면 수백만 행이라 노트북에서 몇 초다.

## 파일을 어떻게 찾는가

폴더를 glob하면 안 된다. 실측에서 `dim_artist` 폴더의 parquet 252개 중
**살아 있는 것이 45개**였다. 나머지는 덮어쓰기로 교체됐거나 강제 종료가
남긴 것이다. 트랜잭션 로그를 읽어 활성 파일만 고른다(flux.common.deltalog).

    venv/Scripts/python scripts/oracle_mart.py --week 2026-09-01
"""

import argparse
import sys
from datetime import date, timedelta

import duckdb

from flux.common import config, deltalog
from flux.gold import fact, mart


def months_touched(start: date, days: int) -> list:
    """이 기간이 걸치는 파티션(달). 주가 달을 넘어가면 둘이 된다."""
    seen = []
    for offset in range(days):
        key = (start + timedelta(days=offset)).strftime("%Y-%m")
        if key not in seen:
            seen.append(key)
    return seen


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--week", default="2026-09-01",
                        help="대조할 주의 첫날 (YYYY-MM-DD)")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--top", type=int, default=5)
    args = parser.parse_args()

    start = date.fromisoformat(args.week)
    end = start + timedelta(days=args.days)
    months = months_touched(start, args.days)

    fact_path = config.local_root() / "gold" / fact.TABLE_NAME
    files = deltalog.files_in_partitions(
        fact_path, fact.PARTITION_COLUMN, months)
    if not files:
        print(f"그 기간의 파티션이 없다: {months}")
        return 1

    print(f"기간      : {start} ~ {end - timedelta(days=1)} ({args.days}일)")
    print(f"파티션(달): {', '.join(months)}")
    print(f"읽을 파일 : {len(files)}개  "
          f"(활성 {len(deltalog.active_files(fact_path))}개 중)")
    print()

    listed = ", ".join(f"'{f.replace(chr(92), '/')}'" for f in files)
    con = duckdb.connect()

    # 팩트의 grain은 청취-아티스트 쌍이다. 줄 수와 청취 수를 따로 센다.
    totals = con.sql(f"""
        SELECT count(*) AS rows,
               count(DISTINCT (_source_dump, _record_id)) AS listens,
               count(DISTINCT artist_mbid) AS artists
        FROM read_parquet([{listed}])
        WHERE listened_at >= TIMESTAMP '{start}'
          AND listened_at <  TIMESTAMP '{end}'
          AND artist_mbid IS NOT NULL
    """).fetchone()

    print(f"팩트 줄   : {totals[0]:,}")
    print(f"청취      : {totals[1]:,}")
    print(f"아티스트  : {totals[2]:,}")
    print()

    top = con.sql(f"""
        SELECT artist_mbid, count(*) AS listens
        FROM read_parquet([{listed}])
        WHERE listened_at >= TIMESTAMP '{start}'
          AND listened_at <  TIMESTAMP '{end}'
          AND artist_mbid IS NOT NULL
        GROUP BY artist_mbid
        ORDER BY listens DESC
        LIMIT {args.top}
    """).fetchall()

    print(f"상위 {args.top}개 아티스트 (DuckDB가 센 것)")
    for mbid, n in top:
        print(f"  {mbid}  {n:>10,}")

    # 일자별 합계. Spark가 만든 마트와 줄 단위로 맞춰 볼 값이다.
    daily = con.sql(f"""
        SELECT CAST(listened_at AS DATE) AS d, count(*) AS listens
        FROM read_parquet([{listed}])
        WHERE listened_at >= TIMESTAMP '{start}'
          AND listened_at <  TIMESTAMP '{end}'
          AND artist_mbid IS NOT NULL
        GROUP BY d ORDER BY d
    """).fetchall()

    print()
    print("일자별")
    for day, n in daily:
        print(f"  {day}  {n:>10,}")

    # Spark가 만든 마트와 줄 단위로 맞춘다. 여기까지 와야 게이트다.
    mart_path = config.local_root() / "gold" / mart.DAILY_ARTIST
    mismatches = []
    compared = 0
    if mart_path.is_dir():
        mart_files = deltalog.active_paths(mart_path)
        mart_listed = ", ".join(
            f"'{f.replace(chr(92), '/')}'" for f in mart_files)
        spark_daily = dict(con.sql(f"""
            SELECT CAST(listened_date AS DATE) AS d, sum(listens) AS listens
            FROM read_parquet([{mart_listed}])
            WHERE listened_date >= DATE '{start}'
              AND listened_date <  DATE '{end}'
            GROUP BY d ORDER BY d
        """).fetchall())

        print()
        print(f"Spark 마트와 대조 (활성 파일 {len(mart_files)}개)")
        print(f"  {'날짜':12} {'DuckDB':>12} {'Spark':>12}  판정")
        for day, mine in daily:
            theirs = spark_daily.get(day)
            compared += 1
            ok = theirs == mine
            if not ok:
                mismatches.append({"date": str(day), "duckdb": mine,
                                   "spark": theirs})
            print(f"  {str(day):12} {mine:>12,} "
                  f"{(theirs if theirs is not None else 0):>12,}  "
                  f"{'일치' if ok else '불일치'}")
        extra = set(spark_daily) - {d for d, _ in daily}
        for day in sorted(extra):
            mismatches.append({"date": str(day), "duckdb": 0,
                               "spark": spark_daily[day]})
            print(f"  {str(day):12} {0:>12,} {spark_daily[day]:>12,}  불일치")
    else:
        print()
        print(f"Spark 마트가 없다: {mart_path}")
        print("scripts/gold_mart.py를 먼저 돌린다. 대조는 건너뛴다.")

    config.write_measurement("oracle-mart", {
        "engine": f"duckdb {duckdb.__version__}",
        "week_start": str(start),
        "days": args.days,
        "months": months,
        "files_read": len(files),
        "fact_rows": totals[0],
        "listens": totals[1],
        "artists": totals[2],
        "top": [{"artist_mbid": m, "listens": n} for m, n in top],
        "daily": [{"date": str(d), "listens": n} for d, n in daily],
        "compared_days": compared,
        "mismatches": mismatches,
    })

    print()
    if not mart_path.is_dir():
        return 0
    if mismatches:
        print(f"대조 실패 : {len(mismatches)}일이 어긋난다")
        return 1
    print(f"대조 통과 : {compared}일 전부 두 엔진이 같은 값을 냈다")
    return 0


if __name__ == "__main__":
    sys.exit(main())
