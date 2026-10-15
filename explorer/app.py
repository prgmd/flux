"""Flux 탐색 화면. 나만 보는 로컬 도구이고 표본 파일을 읽기 전용으로 연다.

    explorer/.venv/Scripts/streamlit run explorer/app.py

표본 파일은 `venv/Scripts/python explorer/build_sample.py`가 만든다. 주소에 `?user=이름&ts=초&msid=번호`를
붙이면 그 청취를 바로 연다. 이 화면은 새로 계산하지 않고
이미 있는 표를 보여 주기만 한다.
"""

import os
import sys
import time
from pathlib import Path

import duckdb
import streamlit as st

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "src"))

from flux.common import queryrun  # noqa: E402
from flux.explorer import provenance, trace  # noqa: E402

st.set_page_config(page_title="Flux 탐색 화면", layout="wide")

ORDER_TEXT = (
    "1순위 출처: MAPPING, CLIENT, LASTFM, NONE 순으로 앞선 줄",
    "2순위 아티스트 수: 많은 줄",
    "3순위 덤프: 나중 덤프",
    "4순위 줄 번호: 작은 줄",
)


def sample_path():
    root = os.environ.get("FLUX_STORAGE_ROOT")
    if not root:
        env_file = REPO / ".env"
        if env_file.exists():
            root = queryrun.env_value(env_file.read_text(encoding="utf-8"), "FLUX_STORAGE_ROOT")
    if not root:
        st.error("FLUX_STORAGE_ROOT가 없다. 환경 변수나 .env에 데이터 루트를 적는다.")
        st.stop()
    return Path(root) / "explorer" / "sample.duckdb"


@st.cache_resource
def connection():
    path = sample_path()
    if not path.exists():
        st.error(f"표본 파일이 없다: {path}\n\n`venv/Scripts/python explorer/build_sample.py`로 만든다.")
        st.stop()
    return duckdb.connect(str(path), read_only=True)


def cursor():
    cur = connection().cursor()
    cur.execute("SET TimeZone='UTC'")
    return cur


def short_dump(name):
    return name.replace("listenbrainz-dump-", "").replace("-incremental", "")


def fmt(value):
    if value is None:
        return "(없음)"
    if isinstance(value, list):
        return ", ".join(str(v) for v in value) if value else "(빈 배열)"
    return str(value)


def matches(silver_value, candidate):
    if candidate is None:
        return False
    left = [str(v).lower() for v in silver_value] if isinstance(silver_value, list) else [str(silver_value).lower()]
    right = [str(v).lower() for v in candidate] if isinstance(candidate, list) else [str(candidate).lower()]
    return bool(right) and left == right


def render_trace(cur, user, row):
    result = trace.trace(cur, user, row["ts"], row["recording_msid"])
    silver = result["silver"]
    copies = result["copies"]

    st.header(f"{silver['artist_name']} - {silver['track_name']}")
    st.caption(f"{user}, {silver['listened_at']} UTC, 제출 클라이언트 {silver['client_name']}")

    st.subheader("1. Bronze: 받은 그대로")
    st.write(f"같은 (사용자, 시각, msid)로 원본 줄이 **{len(copies)}개** 있었다.")
    for copy in copies:
        mark = "남은 줄" if copy["kept"] else "지워진 줄"
        title = f"{mark}: 덤프 {short_dump(copy['_source_dump'])}, 줄 번호 {copy['_record_id']}"
        with st.expander(title, expanded=copy["kept"]):
            st.json(copy["document"])
    others = result["same_time_other_msid"]
    if others:
        st.warning(f"같은 시각에 msid가 다른 줄이 {len(others)}개 있다. 곡이 같으면 같은 청취의 다른 번호일 수 있다.")
        st.dataframe(
            [{"msid": o["msid"], "아티스트": o["artist_name"], "곡": o["track_name"],
              "덤프": short_dump(o["_source_dump"]), "줄 번호": o["_record_id"]} for o in others],
            hide_index=True, width="stretch")

    st.subheader("2. Silver: 칸이 JSON의 어디서 왔나")
    st.caption("남은 줄의 JSON에서 읽은 값이다. 표시한 값이 Silver에 들어간 값과 같은 경로다.")
    for column in result["columns"]:
        name, value, paths = st.columns([1.2, 2.2, 4.6])
        name.markdown(f"**{column['column']}**")
        value.code(fmt(column["silver_value"]), language=None)
        lines = [
            f"- {'✔ ' if matches(column['silver_value'], found) else ''}`{path}`: `{fmt(found)}`"
            for path, found in column["candidates"]
        ]
        paths.markdown("\n".join(lines) + f"\n\n_{column['rule']}_")
    st.write(f"**mbid_source** = `{silver['mbid_source']}`, 파티션 `{silver['listened_month']}`, "
             f"적재 `{silver['_ingested_at']}`, Silver 생성 `{silver['_built_at']}`")

    st.subheader("3. 중복 제거: 어느 줄이 남았나")
    if len(copies) == 1:
        st.write("같은 키의 다른 줄이 없어 그대로 올라왔다.")
    else:
        table = []
        for copy in copies:
            source, artists = provenance.resolve_source(copy["document"])
            info = copy["document"].get("track_metadata", {}).get("additional_info", {})
            table.append({
                "결과": "남음" if copy["kept"] else "지워짐",
                "덤프": short_dump(copy["_source_dump"]),
                "줄 번호": copy["_record_id"],
                "MBID 출처(어림)": source,
                "아티스트 수(어림)": artists,
                "클라이언트": info.get("submission_client"),
            })
        st.dataframe(table, hide_index=True, width="stretch")
        st.markdown("남길 줄은 이 순서로 고른다.\n\n" + "\n".join(f"- {line}" for line in ORDER_TEXT))
        st.caption("출처와 아티스트 수는 JSON에서 읽은 값으로 어림한 것이다(UUID 형식 검사는 하지 않았다).")

    st.subheader("4. Gold 팩트: 청취와 아티스트의 쌍")
    for fact in result["facts"]:
        if fact["artist_mbid"] is None:
            st.write("아티스트 MBID가 없어 `NULL` 한 줄로 남았다.")
            continue
        st.markdown(f"팩트 한 줄: 아티스트 `{fact['artist_mbid']}`")
        versions = fact["versions"]
        st.dataframe(
            [{"": "← 이 청취 때 유효" if index == fact["active"] else "", "이름": v["name"],
              "시작": v["valid_from"], "끝": v["valid_to"], "현재": v["is_current"]}
             for index, v in enumerate(versions)],
            hide_index=True, width="stretch")
        if fact["active"] is not None:
            st.success(f"{silver['listened_date']}에 이 아티스트의 이름은 **{versions[fact['active']]['name']}**")


def page_trace():
    cur = cursor()
    users = cur.execute("select user_name, kind, listens from sample_users order by kind, user_name").fetchall()
    labels = {f"{name}  ({kind}, {count:,}건)": name for name, kind, count in users}
    params = st.query_params
    names = list(labels.values())
    start = names.index(params["user"]) if params.get("user") in names else 0
    user = labels[st.selectbox("사용자", list(labels), index=start)]

    left, middle, right = st.columns(3)
    text = left.text_input("아티스트나 곡 이름에 포함")
    clients = [r[0] for r in cur.execute(
        "select distinct client_name from silver where user_name = ? order by 1", [user]).fetchall()]
    client = middle.selectbox("클라이언트", ["전체"] + clients)
    duplicated = right.checkbox("원본에 같은 줄이 여러 개였던 것만")

    rows = trace.listens(cur, user, text=text or None, client=None if client == "전체" else client)
    if duplicated:
        rows = [r for r in rows if (r["copies"] or 1) > 1]
    st.caption(f"{len(rows):,}건(최근 순, 최대 2,000건). 줄을 눌러 고른다.")
    event = st.dataframe(
        [{"시각": r["time"], "아티스트": r["artist_name"], "곡": r["track_name"], "클라이언트": r["client_name"],
          "MBID 출처": r["mbid_source"], "원본 줄 수": r["copies"]} for r in rows],
        hide_index=True, width="stretch", on_select="rerun", selection_mode="single-row")
    picked = event.selection.rows
    if picked:
        render_trace(cur, user, rows[picked[0]])
    elif params.get("user") == user and params.get("ts") and params.get("msid"):
        render_trace(cur, user, {"ts": int(params["ts"]), "recording_msid": params["msid"]})
    else:
        st.info("위 표에서 한 줄을 고르면 그 청취가 Bronze, Silver, Gold를 지나는 모습이 나온다.")


def run_timed(cur, statement, repeat):
    """질의를 repeat번 돌려 마지막 결과와 회마다 걸린 초를 돌려준다. 결과는 최대 1,000줄까지 가져온다."""
    seconds = []
    frame = None
    for _ in range(repeat):
        started = time.perf_counter()
        frame = cur.sql(statement).limit(1000).df()
        seconds.append(time.perf_counter() - started)
    return frame, seconds


def page_sql():
    st.write("읽기 전용. 표본의 표: `bronze`, `silver`, `fact`, `dim_artist`, `quarantine`, `sample_users`. "
             "전체 데이터가 아니라 표본이다.")
    sql = st.text_area("SQL", "select * from silver limit 10", height=170)
    left, right = st.columns([1, 4])
    repeat = int(left.number_input("반복 횟수", min_value=1, max_value=10, value=1))
    right.caption("여러 번 돌리면 첫 회(캐시가 빈 상태)와 이후의 시간을 비교할 수 있다.")
    if not st.button("실행"):
        return
    if not queryrun.is_read_only(sql):
        st.error("읽기 전용 질의(SELECT, WITH, FROM, DESCRIBE, SHOW, EXPLAIN, SUMMARIZE)만 실행한다.")
        return
    cur = cursor()
    for statement in queryrun.statements(sql):
        try:
            frame, seconds = run_timed(cur, statement, repeat)
        except Exception as error:
            st.error(f"{type(error).__name__}: {error}")
            return
        runs = ", ".join(f"{value:.3f}" for value in seconds)
        summary = f"{len(frame):,}줄(최대 1,000줄까지 표시), {runs}초" if repeat > 1 else             f"{len(frame):,}줄(최대 1,000줄까지 표시), {seconds[0]:.3f}초"
        if repeat > 1:
            summary += f", 가장 느린 회 {max(seconds):.3f}초, 가장 빠른 회 {min(seconds):.3f}초"
        st.caption(summary)
        st.dataframe(frame, width="stretch")


def page_about():
    cur = cursor()
    st.write("이 화면은 `explorer/build_sample.py`가 만든 표본 파일을 읽는다. 사용자 몇십 명의 줄을 세 층 전부 담았다.")
    counts = {name: cur.execute(f"select count(*) from {name}").fetchone()[0]
              for name in ("bronze", "silver", "fact", "dim_artist", "quarantine", "sample_users")}
    st.dataframe([{"표": name, "줄 수": rows} for name, rows in counts.items()], hide_index=True)
    st.subheader("표본 사용자")
    st.dataframe(
        cur.execute("select kind as 유형, user_name as 사용자, listens as 청취 from sample_users order by kind, user_name").df(),
        hide_index=True, width="stretch")
    st.caption(f"Bronze와 Silver의 차이 {counts['bronze'] - counts['silver']:,}줄은 중복 제거로 사라진 줄이다.")


PAGES = {"청취 한 줄 따라가기": page_trace, "SQL 창": page_sql, "표본 안내": page_about}
PAGES[st.sidebar.radio("화면", list(PAGES))]()
