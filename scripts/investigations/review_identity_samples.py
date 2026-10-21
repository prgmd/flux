import argparse
import hashlib
import json
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.silver.name_candidates import POLICY_VERSION, candidate_key


CASES = {
    'msid_split': "resolved_mbid='d9b46ecb-5472-4dcd-8fa4-dd6723189e27' and lower(track_name)='reckoner'",
    'msid_conflict': "recording_msid='000ff079-136d-4869-ab0e-6885078e2141' and resolved_mbid is not null",
    'name_version_collision': "lower(track_name)='get lucky' and resolved_mbid in ('833f00e1-781f-4edd-90e4-e52712618862','dbeffc1d-8f04-483e-8d02-270ad4922b28')",
    'name_hidden_live': "lower(track_name)='reckoner' and resolved_mbid in ('d9b46ecb-5472-4dcd-8fa4-dd6723189e27','694065b4-3910-49c8-a4dd-cb5d270201a1')",
    'name_accent_split': "resolved_mbid='0a65e058-83c4-4982-9a5e-d2495fc4565c' and lower(track_name)='irreplaceable'",
    'name_hyphen_split': "resolved_mbid='4a154661-f61a-4ea0-b88f-fe89ae5d3afc' and lower(track_name)='breezeblocks'",
    'missing_reference': "lower(strip_accents(artist_name))='beyonce' and lower(track_name)='irreplaceable' and recording_mbid is null",
    'remix_boundary': "resolved_mbid='21cdf196-6418-40b9-b633-d5868aa9c727'",
}


def fingerprint(root):
    return {n:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(root/n))).hexdigest()
            for n in ('recording_metadata','monthly_user_recordings')}


def run(prepared, silver, output):
    prepared,silver,output=(p.resolve() for p in (prepared,silver,output))
    if any(output==p or p in output.parents or output in p.parents for p in (prepared,silver)):
        raise ValueError('overlapping output')
    prior=json.loads((prepared/'result.json').read_text())
    before=fingerprint(silver)
    if before!=prior['source_before']['silver']:
        raise ValueError('prepared source mismatch')
    output.mkdir(parents=True,exist_ok=False)
    c=duckdb.connect(str(prepared/'experiment.duckdb'),read_only=True)
    c.execute('set threads=2');c.execute("set memory_limit='4GB'")
    c.execute("""create temp table targets as select distinct recording_msid,artist_name,track_name,recording_mbid
        from metadata where lower(strip_accents(trim(artist_name))) in
        ('radiohead','billie eilish','beyonce','daft punk','rm','alt-j','alt‐j','jung kook','jungkook')""")
    c.execute("""create temp table evidence as select m.*,x.resolved_mbid,r.recording_title,r.comment,a.credit_display
        from targets m left join resolution x on m.recording_mbid=x.original_mbid
        left join recordings r on x.resolved_mbid=r.recording_mbid
        left join credit_displays a using(artist_credit)""")
    result={'completed':False,'scope':'purpose-selected examples, not a population accuracy estimate',
            'source_before':before,'preprocessing_policy':POLICY_VERSION,'cases':{}}
    report=output/'result.json'
    report.write_text(json.dumps(result,indent=2),encoding='utf-8')
    for name,condition in CASES.items():
        query=f"""select * from evidence where {condition}
            qualify row_number() over(partition by resolved_mbid,artist_name order by recording_msid)<=2
            order by resolved_mbid,artist_name,recording_msid limit 12"""
        cursor=c.execute(query);columns=[d[0] for d in cursor.description]
        rows=[dict(zip(columns,r)) for r in cursor.fetchall()]
        if not rows:
            raise ValueError('empty sample: '+name)
        for row in rows:
            row['candidate_key']=candidate_key(row['artist_name'],row['track_name'])
            row['basic_key']=[row['artist_name'].strip().lower(),row['track_name'].strip().lower()]
        result['cases'][name]=rows
    refs=sorted({r['resolved_mbid'] for rows in result['cases'].values() for r in rows if r['resolved_mbid']})
    raw=c.execute("select gid,name,comment from raw_recording where gid in(select unnest(?))",[refs]).fetchall()
    original={gid:(title,comment) for gid,title,comment in raw}
    for rows in result['cases'].values():
        for row in rows:
            if row['resolved_mbid'] and original.get(row['resolved_mbid'])!=(row['recording_title'],row['comment']):
                raise ValueError('official snapshot mismatch')
    result['official_artist']=c.execute("""select n.artist_credit,a.gid,a.name from raw_artist_credit_name n
        join raw_artist a on n.artist=a.id where n.artist_credit='49627'""").fetchall()
    if result['official_artist']!=[('49627','859d0860-d480-4efd-970c-c05d5f1776b8','Beyoncé')]:
        raise ValueError('artist alias evidence mismatch')
    result['targeted_metadata_rows']=c.execute('select count(*) from targets').fetchone()[0]
    result['sample_cases']=len(CASES)
    result['sample_rows']=sum(len(r) for r in result['cases'].values())
    result['official_reference_recordings_checked']=len(refs)
    c.close()
    result['source_after']=fingerprint(silver)
    if result['source_after']!=before:
        raise ValueError('source changed')
    result['completed']=True;result['source_unchanged']=True
    report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='cases'},ensure_ascii=False))


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for n in ('prepared','silver','output'):
        p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();run(a.prepared,a.silver,a.output)
