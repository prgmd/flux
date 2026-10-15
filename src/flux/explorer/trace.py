"""청취 한 줄이 Bronze, Silver, Gold를 지나는 모습을 표본 파일에서 꺼낸다.

화면(explorer/app.py)은 이 모듈이 돌려주는 값을 그리기만 한다. 연결에는 `bronze`, `silver`,
`fact`, `dim_artist` 표가 있어야 한다(flux.explorer.sample이 만든다). 읽기만 한다.
"""

import json

from flux.explorer import provenance

MSID = "json_extract_string(raw_json, '$.recording_msid')"
SILVER_COLUMNS = (
    "listened_month, user_id, user_name, recording_msid, recording_mbid, artist_mbids, release_mbid, "
    "mbid_source, client_name, track_name, artist_name, release_name, duration_ms, _source_dump, _record_id"
)


def version_at(versions, date_text):
    """차원 이력에서 그 날짜에 유효했던 줄의 위치. 없으면 None.

    시작은 포함하고 끝은 제외한다(flux.gold.scd2.covers와 같다). 날짜는 `YYYY-MM-DD` 글자다.
    """
    for index, version in enumerate(versions):
        if version["valid_from"] <= date_text < version["valid_to"]:
            return index
    return None


def listens(con, user_name, *, text=None, client=None, limit=2000):
    """한 사용자의 청취 목록. 원본 줄 수(copies)는 같은 (시각, msid)가 Bronze에 몇 줄 있었는지다."""
    conditions = ["s.user_name = ?"]
    filters = [user_name]
    if text:
        conditions.append("(lower(s.artist_name) like ? or lower(s.track_name) like ?)")
        pattern = f"%{text.lower()}%"
        filters += [pattern, pattern]
    if client:
        conditions.append("s.client_name = ?")
        filters.append(client)
    sql = f"""
        with copies as (
            select listened_at as ts, {MSID} as msid, count(*) as copies
            from bronze
            where user_name = ?
            group by 1, 2
        )
        select epoch(s.listened_at)::bigint as ts,
               strftime(s.listened_at, '%Y-%m-%d %H:%M:%S') as time,
               s.artist_name, s.track_name, s.client_name, s.mbid_source,
               c.copies, s.recording_msid
        from silver s
        left join copies c on c.ts = epoch(s.listened_at)::bigint and c.msid = s.recording_msid
        where {" and ".join(conditions)}
        order by s.listened_at desc
        limit ?
    """
    return _rows(con, sql, [user_name] + filters + [int(limit)])


def _rows(con, sql, params):
    cursor = con.execute(sql, params)
    columns = [d[0] for d in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def trace(con, user_name, ts, msid):
    """키 (사용자, 청취 시각, msid)의 청취를 세 층에서 찾아 모은다."""
    silver = _rows(
        con,
        f"select {SILVER_COLUMNS}, strftime(listened_at, '%Y-%m-%d %H:%M:%S') as listened_at, "
        "cast(_ingested_at as varchar) as _ingested_at, cast(_built_at as varchar) as _built_at, "
        "strftime(listened_at, '%Y-%m-%d') as listened_date from silver "
        "where user_name = ? and epoch(listened_at)::bigint = ? and recording_msid = ?",
        [user_name, ts, msid],
    )
    if not silver:
        raise LookupError(f"Silver에 그 청취가 없다: {user_name}, {ts}, {msid}")
    kept = silver[0]
    key = (kept["_source_dump"], kept["_record_id"])

    copies = _rows(
        con,
        f"select _source_dump, _record_id, raw_json from bronze "
        f"where user_name = ? and listened_at = ? and {MSID} = ? order by _source_dump, _record_id",
        [user_name, ts, msid],
    )
    for copy in copies:
        copy["document"] = json.loads(copy["raw_json"])
        copy["kept"] = (copy["_source_dump"], copy["_record_id"]) == key
    survivor = next(copy for copy in copies if copy["kept"])

    same_time = _rows(
        con,
        f"select _source_dump, _record_id, {MSID} as msid, "
        "json_extract_string(raw_json, '$.track_metadata.artist_name') as artist_name, "
        "json_extract_string(raw_json, '$.track_metadata.track_name') as track_name "
        f"from bronze where user_name = ? and listened_at = ? and {MSID} <> ? "
        "order by _source_dump, _record_id",
        [user_name, ts, msid],
    )

    columns = []
    for column, paths, rule in provenance.PROVENANCE:
        columns.append({
            "column": column,
            "silver_value": kept.get(column),
            "candidates": [(path, provenance.json_path_value(survivor["document"], path)) for path in paths],
            "rule": rule,
        })

    facts = _rows(
        con,
        "select artist_mbid from fact where _source_dump = ? and _record_id = ? order by artist_mbid nulls first",
        list(key),
    )
    for fact in facts:
        versions = _rows(
            con,
            "select name, valid_from, valid_to, is_current from dim_artist where artist_mbid = ? order by valid_from",
            [fact["artist_mbid"]],
        ) if fact["artist_mbid"] else []
        fact["versions"] = versions
        fact["active"] = version_at(versions, kept["listened_date"])

    return {
        "silver": kept,
        "copies": copies,
        "same_time_other_msid": same_time,
        "columns": columns,
        "facts": facts,
    }
