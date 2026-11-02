import argparse
import hashlib
import importlib.util
import json
import time
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.silver.identity_experiment import POLICIES, compare, register


def digest(path):
    h=hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda:stream.read(8*1024*1024),b''):
            h.update(block)
    return h.hexdigest()


def prepare(c):
    c.execute("""create table reference_observations as select distinct
        m.recording_msid,m.artist_name artist,m.track_name title,r.recording_mbid reference_mbid,
        r.recording_title,r.comment,a.credit_display,
        exists(select 1 from credit_names n where n.artist_credit=r.artist_credit
            and name_clean(m.artist_name)=name_clean(n.name)) artist_matches,
        name_clean(m.track_name)=name_clean(r.recording_title) title_matches
        from metadata m join resolution x on m.recording_mbid=x.original_mbid
        join recordings r on x.resolved_mbid=r.recording_mbid
        join credit_displays a using(artist_credit)
        where nullif(trim(m.artist_name),'') is not null and nullif(trim(m.track_name),'') is not null""")


def run(silver, snapshot, redirects, output):
    silver,snapshot,redirects,output=(p.resolve() for p in (silver,snapshot,redirects,output))
    if any(p==output or p in output.parents or output in p.parents for p in (silver,snapshot,redirects)):
        raise ValueError('overlapping output')
    def state():
        return {'silver':{n:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(silver/n))).hexdigest()
                          for n in ('recording_metadata','monthly_user_recordings')},
                'official':digest(snapshot/'result.json'),'redirects':digest(redirects/'result.json')}
    official=json.loads((snapshot/'result.json').read_text())
    redirected=json.loads((redirects/'result.json').read_text())
    if not official['completed'] or not redirected['completed'] or official['source_release']!=redirected['source_release']:
        raise ValueError('official snapshot mismatch')
    for root,manifest in ((snapshot,official),(redirects,redirected)):
        for name,entry in manifest['tables'].items():
            if digest(root/name)!=entry['sha256']:
                raise ValueError('checksum mismatch: '+name)
    before=state()
    output.mkdir(parents=True,exist_ok=False)
    start=time.monotonic()
    result={'completed':False,'hypothetical_only':True,'source_before':before,'policies':{}}
    def save():
        (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    save()
    spec=importlib.util.spec_from_file_location('official_loader',Path(__file__).with_name('audit_official_credits.py'))
    loader=importlib.util.module_from_spec(spec);spec.loader.exec_module(loader)
    c=duckdb.connect(str(output/'experiment.duckdb'))
    c.execute('set threads=2');c.execute("set memory_limit='8GB'")
    c.execute("set max_temp_directory_size='50GB'");c.execute('set preserve_insertion_order=false')
    register(c)
    c.read_parquet(deltalog.active_paths(silver/'recording_metadata'),hive_partitioning=True).create_view('metadata')
    c.execute("create table mbids as select distinct recording_mbid from metadata where recording_mbid is not null")
    loader.load_tsv(c,redirects,'recording_gid_redirect',('gid','new_id','created'))
    loader.load_tsv(c,snapshot,'recording',('id','gid','name','artist_credit','length','comment','edits_pending','last_updated','video'))
    c.execute('create table relevant_redirects as select gid,new_id from raw_recording_gid_redirect where gid in(select recording_mbid from mbids)')
    if c.execute('select count(*)-count(distinct gid) from relevant_redirects').fetchone()[0]:
        raise ValueError('ambiguous redirect')
    c.execute("""create table selected_recordings as select * from raw_recording
        where gid in(select recording_mbid from mbids) or id in(select new_id from relevant_redirects)""")
    c.execute("""create table resolution as select p.recording_mbid original_mbid,
        coalesce(d.gid,t.gid) resolved_mbid from mbids p
        left join selected_recordings d on p.recording_mbid=d.gid
        left join relevant_redirects x on p.recording_mbid=x.gid
        left join selected_recordings t on x.new_id=t.id""")
    if c.execute('select count(*)-count(distinct original_mbid) from resolution').fetchone()[0]:
        raise ValueError('duplicate resolution')
    c.execute('create table recordings as select gid recording_mbid,name recording_title,artist_credit,comment from selected_recordings')
    for n,cols in [('artist',loader.TABLE_COLUMNS['artist']),
                   ('artist_credit',('id','name','artist_count','ref_count','created','edits_pending','gid')),
                   ('artist_credit_name',('artist_credit','position','artist','name','join_phrase'))]:
        loader.load_tsv(c,snapshot,n,cols)
    c.execute("""create table credit_displays as select id artist_credit,name credit_display from raw_artist_credit
        where id in(select artist_credit from recordings)""")
    c.execute("""create table credit_names as select artist_credit,credit_display name from credit_displays
        union select n.artist_credit,n.name from raw_artist_credit_name n semi join credit_displays using(artist_credit)
        union select n.artist_credit,a.name from raw_artist_credit_name n join raw_artist a on n.artist=a.id
        semi join credit_displays using(artist_credit)""")
    print('official reference loaded',flush=True)
    prepare(c)
    result['reference']={'observations':c.execute('select count(*) from reference_observations').fetchone()[0],
        'msids':c.execute('select count(distinct recording_msid) from reference_observations').fetchone()[0],
        'recordings':c.execute('select count(distinct reference_mbid) from reference_observations').fetchone()[0]}
    cohorts={'official_available':'true','artist_confirmed':'artist_matches',
             'title_matched_sensitivity':'artist_matches and title_matches'}
    for cohort,condition in cohorts.items():
        c.execute(f'create or replace temp view observations as select * from reference_observations where {condition}')
        result['policies'][cohort]={}
        for policy in POLICIES:
            stats=compare(c,policy)
            result['policies'][cohort][policy]=stats
            print(cohort,policy,json.dumps(stats),flush=True);save()
            if cohort=='artist_confirmed':
                groups=c.execute('''select candidate,count(*) refs,sum(n) observations from cells
                    group by candidate having count(*)>1 order by observations desc,candidate limit 30''').fetchall()
                examples=[]
                for key,refs,n in groups:
                    query=f"""select distinct recording_msid,artist,title,reference_mbid,recording_title,comment,credit_display
                        from observations where {POLICIES[policy]}=? order by reference_mbid,recording_msid limit 12"""
                    cursor=c.execute(query,[key]);cols=[d[0] for d in cursor.description]
                    examples.append({'candidate':key,'references':refs,'observations':n,
                                     'rows':[dict(zip(cols,r)) for r in cursor.fetchall()]})
                (output/(policy+'-conflicts.json')).write_text(json.dumps(examples,ensure_ascii=False,indent=2),encoding='utf-8')
        save()
    c.execute('create or replace temp view observations as select * from reference_observations where artist_matches')
    samples=c.execute("""select reference_mbid,count(distinct recording_msid) msids,
        count(distinct to_json(struct_pack(artist:=name_clean(artist),track:=name_clean(title)))) names,
        min(recording_title) title,min(credit_display) credit
        from observations group by reference_mbid having msids>1
        order by msids desc,reference_mbid limit 30""")
    columns=[d[0] for d in samples.description]
    result['fragmentation_examples']=[dict(zip(columns,r)) for r in samples.fetchall()]
    result['source_after']=state()
    result['source_unchanged']=before==result['source_after']
    if not result['source_unchanged']:
        raise ValueError('input changed')
    result['seconds']=round(time.monotonic()-start,2)
    result['completed']=True
    c.close();save()
    print('completed',result['seconds'],flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for n in ('silver','snapshot','redirects','output'):
        p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();run(a.silver,a.snapshot,a.redirects,a.output)
