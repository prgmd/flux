import argparse
import hashlib
import importlib.util
import json
import time
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.gold.artist_attribution import classify
from flux.gold.monthly_marts import literal


def run(silver, audit, snapshot, redirects, output):
    silver,audit,snapshot,redirects,output=(p.resolve() for p in (silver,audit,snapshot,redirects,output))
    if any(output==p or p in output.parents or output in p.parents for p in (silver,audit,snapshot,redirects)):
        raise ValueError('overlapping output')
    def fingerprint():
        return {n:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(silver/n))).hexdigest()
                for n in ('recording_metadata','monthly_user_recordings')}
    info=json.loads((audit/'result.json').read_text())
    source=json.loads((snapshot/'result.json').read_text())
    redirect_info=json.loads((redirects/'result.json').read_text())
    before=fingerprint()
    if not info['completed'] or not info['source_unchanged'] or before!=info['source_after']:
        raise ValueError('stale audit')
    if not source['completed'] or not redirect_info['completed'] or source['source_release']!=redirect_info['source_release']:
        raise ValueError('inconsistent official snapshots')
    if hashlib.sha256((redirects/'recording_gid_redirect').read_bytes()).hexdigest()!=redirect_info['tables']['recording_gid_redirect']['sha256']:
        raise ValueError('redirect checksum mismatch')
    output.mkdir(parents=True,exist_ok=False)
    start=time.monotonic()
    spec=importlib.util.spec_from_file_location('official_audit',Path(__file__).with_name('audit_official_credits.py'))
    loader=importlib.util.module_from_spec(spec);spec.loader.exec_module(loader)
    c=duckdb.connect(str(output/'audit.duckdb'))
    c.execute('set threads=2');c.execute("set memory_limit='6GB'")
    c.read_parquet(str(audit/'reviewed.parquet')).create_view('prior')
    c.read_parquet(deltalog.active_paths(silver/'monthly_user_recordings'),hive_partitioning=True).create_view('monthly')
    loader.load_tsv(c,redirects,'recording_gid_redirect',('gid','new_id','created'))
    loader.load_tsv(c,snapshot,'recording',('id','gid','name','artist_credit','length','comment','edits_pending','last_updated','video'))
    c.execute("""create table relevant_redirects as select gid,new_id from raw_recording_gid_redirect
        where gid in(select recording_mbid from prior where mbids=1)""")
    if c.execute('select count(*)-count(distinct gid) from relevant_redirects').fetchone()[0]:
        raise ValueError('ambiguous redirect')
    c.execute("""create table selected_recordings as select * from raw_recording
        where gid in(select recording_mbid from prior where mbids=1)
        or id in(select new_id from relevant_redirects)""")
    c.execute("""create table resolution as select p.recording_mbid original_mbid,
        coalesce(d.gid,t.gid) resolved_mbid from (select distinct recording_mbid from prior where mbids=1) p
        left join selected_recordings d on p.recording_mbid=d.gid
        left join relevant_redirects x on p.recording_mbid=x.gid
        left join selected_recordings t on x.new_id=t.id""")
    if c.execute('select count(*)-count(distinct original_mbid) from resolution').fetchone()[0]:
        raise ValueError('duplicate resolution')
    c.execute("""create table recordings as select gid recording_mbid,name recording_title,
        artist_credit,comment,cast(video as boolean) video from selected_recordings""")
    loader.load_tsv(c,snapshot,'artist',loader.TABLE_COLUMNS['artist'])
    loader.load_tsv(c,snapshot,'artist_credit',('id','name','artist_count','ref_count','created','edits_pending','gid'))
    loader.load_tsv(c,snapshot,'artist_credit_name',('artist_credit','position','artist','name','join_phrase'))
    c.execute("""create table credits as select n.artist_credit,a.gid artist_mbid,a.name artist_name,
        n.name credit_name,ac.name credit_display from raw_artist_credit_name n
        join raw_artist a on n.artist=a.id join raw_artist_credit ac on n.artist_credit=ac.id
        where n.artist_credit in(select artist_credit from recordings)""")
    classify(c)
    if c.execute('select count(*)-count(distinct recording_msid) from reviewed').fetchone()[0]:
        raise ValueError('duplicate MSID')
    if c.execute("select count(*) from reviewed where status='matched_candidate' and artist_status<>'accepted'").fetchone()[0]:
        raise ValueError('lost prior accepted credit')
    print('resolved and classified',flush=True)
    c.execute("""create table coverage_comparison as select listened_month,
        sum(play_count)::bigint total,
        coalesce(sum(play_count) filter(where status='matched_candidate'),0)::bigint before_plays,
        coalesce(sum(play_count) filter(where artist_status='accepted'),0)::bigint after_plays,
        coalesce(sum(play_count) filter(where artist_status='accepted' and resolved_mbid<>recording_mbid),0)::bigint redirected_plays
        from monthly join reviewed using(recording_msid) group by listened_month""")
    stats=c.execute('select status,artist_status,sum(play_count)::bigint from monthly join reviewed using(recording_msid) group by status,artist_status order by 3 desc').fetchall()
    for name in ('reviewed','proposed_credits','coverage_comparison','resolution'):
        c.execute(f'copy {name} to {literal(output/(name+".parquet"))} (format parquet,compression zstd)')
    if before!=fingerprint():
        raise ValueError('Silver changed')
    result={'completed':True,'source_before':before,'source_after':before,'source_unchanged':True,
            'source_release':source['source_release'],'policy':'separate-artist-v1',
            'stats':stats,'totals':c.execute('select sum(total),sum(before_plays),sum(after_plays),sum(redirected_plays) from coverage_comparison').fetchone(),
            'seconds':round(time.monotonic()-start,2),'prior_audit':str(audit),
            'identity_policy':'original song identity and version decisions unchanged',
            'credit_policy':'single resolved recording MBID, matching title and official artist credit; version review independent'}
    c.close()
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ('silver','audit','snapshot','redirects','output'):
        p.add_argument('--'+name,type=Path,required=True)
    a=p.parse_args();run(a.silver,a.audit,a.snapshot,a.redirects,a.output)
