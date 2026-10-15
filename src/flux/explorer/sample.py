"""사용자 몇십 명의 줄을 세 층 전부 뽑아 작은 DuckDB 파일 하나에 담는다.

원본 표는 SQL 읽기 식으로 받는다(`delta_scan('...')`이나 테스트의 메모리 표 이름). 읽기만 하고
쓰는 것은 표본 파일뿐이다. 어떤 사용자를 담을지는 flux.explorer.pick이 정한다.

담는 표는 다음과 같다.

    bronze          그 사용자의 Bronze 줄 전부(raw_json 포함)
    silver          그 사용자의 Silver 줄 전부
    fact            그 사용자의 팩트 줄(user_id로 찾는다)
    dim_artist      팩트에 나온 아티스트의 차원 줄 전부(이력 포함)
    quarantine      표본 Bronze 줄에서 격리된 Silver 줄
    sample_users    누가 어떤 유형으로 들어왔나
"""

from pathlib import Path

from flux.explorer.pick import UserStats

SOURCE_NAMES = ("bronze", "silver", "fact", "dim_artist", "quarantine")


def user_stats_sql(silver, bronze, low, high):
    return f"""
    with s as (
        select user_name,
               count(*) as listens,
               avg(case when lower(client_name) like '%import%' then 1.0 else 0.0 end) as importer_share,
               count(distinct client_name) as clients,
               avg(case when len(artist_mbids) > 1 then 1.0 else 0.0 end) as collab_share
        from {silver}
        group by user_name
        having count(*) between {int(low)} and {int(high)}
    ),
    b as (
        select user_name, count(*) as bronze_rows
        from {bronze}
        group by user_name
    )
    select s.user_name, s.listens, s.importer_share, s.clients,
           b.bronze_rows - s.listens as removed_duplicates, s.collab_share
    from s join b using (user_name)
    order by s.user_name
    """


def read_user_stats(con, sources, low, high):
    rows = con.execute(user_stats_sql(sources["silver"], sources["bronze"], low, high)).fetchall()
    return [
        UserStats(name, int(listens), float(importer or 0.0), int(clients), int(removed), float(collab or 0.0))
        for name, listens, importer, clients, removed, collab in rows
    ]


def extract(con, sources, chosen, out_path):
    """chosen은 {사용자 이름: 유형}. 표본 파일을 새로 만들고 표마다 담은 줄 수를 돌려준다."""
    missing = [name for name in SOURCE_NAMES if name not in sources]
    if missing:
        raise ValueError(f"원본 표가 모자란다: {missing}")
    if not chosen:
        raise ValueError("뽑은 사용자가 없다")

    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        out.unlink()

    con.execute("create or replace temp table chosen_users(user_name varchar, kind varchar)")
    con.executemany("insert into chosen_users values (?, ?)", list(chosen.items()))
    con.execute(f"attach '{out.as_posix()}' as sample")
    try:
        bronze, silver, fact = sources["bronze"], sources["silver"], sources["fact"]
        con.execute(f"create table sample.bronze as select * from {bronze} "
                    "where user_name in (select user_name from chosen_users)")
        con.execute(f"create table sample.silver as select * from {silver} "
                    "where user_name in (select user_name from chosen_users)")
        con.execute(f"create table sample.fact as select * from {fact} "
                    "where user_id in (select distinct user_id from sample.silver)")
        con.execute(f"create table sample.dim_artist as select * from {sources['dim_artist']} "
                    "where artist_mbid in (select distinct artist_mbid from sample.fact "
                    "where artist_mbid is not null)")
        con.execute(f"create table sample.quarantine as select * from {sources['quarantine']} "
                    "where (_source_dump, _record_id) in (select _source_dump, _record_id from sample.bronze)")
        con.execute("create table sample.sample_users as "
                    "select c.user_name, c.kind, count(s.user_name) as listens "
                    "from chosen_users c left join sample.silver s using (user_name) "
                    "group by c.user_name, c.kind order by c.kind, c.user_name")
        return {
            name: con.execute(f"select count(*) from sample.{name}").fetchone()[0]
            for name in (*SOURCE_NAMES, "sample_users")
        }
    finally:
        con.execute("detach sample")
