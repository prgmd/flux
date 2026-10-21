import argparse
import hashlib
import json
import re
import unicodedata as ud
from collections import Counter
from pathlib import Path

import duckdb

from flux.silver.name_character_catalog import policy
from flux.silver.name_candidates import POLICY_VERSION
from flux.silver.name_preview import MODES, compare_baseline, preview, review_groups
from flux.common import deltalog


FIELDS = ('recording_msid','artist_name','track_name','recording_mbid','resolved_mbid',
          'recording_title','comment','credit_display')
EXTRA_CASES = {
    'decomposed_unicode': "recording_msid in ('becf1e0c-2ef6-46ed-a77b-dfbc1aefd847','021036c4-1fdb-4dca-a272-25a681b504ab')",
    'apostrophe_and_hidden_live': "lower(artist_name)='radiohead' and lower(track_name) in ('i can’t','i can''t') and resolved_mbid is not null",
    'whitespace': "recording_msid in ('34679bd0-144c-41b9-8563-3b18ec76126e','5111e322-7c5f-463e-98e1-fe521934d607')",
    'latin_ligature': "lower(artist_name)='daft punk' and lower(track_name) in ('phœnix','phoenix') and resolved_mbid='28651acc-1f13-4431-b2de-d93a33a50084'",
    'compatibility_dots_and_live': "lower(artist_name)='radiohead' and (track_name like '%․․%' or lower(track_name)='bullet proof..i wish i was') and resolved_mbid is not null",
    'hangul_compatibility': "recording_msid='b28c6cbc-9e72-436f-8743-a445580cb9b6'",
}


def fingerprint(root):
    return {n:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(root/n))).hexdigest()
            for n in ('recording_metadata','monthly_user_recordings')}


def run(pool,review,prepared,silver,output,baseline=None):
    pool,review,prepared,silver,output=(p.resolve() for p in (pool,review,prepared,silver,output))
    baseline=baseline.resolve() if baseline is not None else None
    inputs=(pool,review,prepared,silver)+((baseline,) if baseline else ())
    for path in inputs:
        if output==path or path in output.parents or output in path.parents:
            raise ValueError('overlapping output')
    old=json.loads(review.read_text(encoding='utf-8'))
    before=fingerprint(silver)
    if not old['completed'] or not old['source_unchanged'] or old['source_after']!=before:
        raise ValueError('sample source mismatch')
    if ud.unidata_version!=policy()['unicode_version']:
        raise ValueError('Unicode version mismatch')
    hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in (pool,review)+((baseline,) if baseline else ())}
    output.mkdir(parents=True,exist_ok=False)
    result={'completed':False,'production_applied':False,'full_corpus_scanned':False,'source_before':before,
            'input_sha256':hashes,'unicode_version':ud.unidata_version,'registry':policy(),
            'candidate_policy_version':POLICY_VERSION}
    report=output/'result.json';report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    selected={}
    def add(row,reason):
        key=tuple(row.get(k) for k in ('recording_msid','artist_name','track_name','recording_mbid'))
        if key not in selected:
            selected[key]={**{k:row.get(k) for k in FIELDS},'sample_reasons':[]}
        if reason not in selected[key]['sample_reasons']:
            selected[key]['sample_reasons'].append(reason)
    for reason,rows in old['cases'].items():
        for row in rows:add(row,reason)
    c=duckdb.connect();c.execute('set threads=2')
    c.read_parquet(str(pool)).create_view('pool')
    for reason,condition in EXTRA_CASES.items():
        cursor=c.execute(f"""select * from pool where {condition}
            qualify row_number() over(partition by resolved_mbid,artist_name,track_name order by recording_msid)=1
            order by resolved_mbid,artist_name,track_name,recording_msid limit 8""")
        columns=[d[0] for d in cursor.description]
        rows=[dict(zip(columns,r)) for r in cursor.fetchall()]
        if not rows:raise ValueError('empty sample: '+reason)
        for row in rows:add(row,reason)
    c.close()
    rows=sorted(selected.values(),key=lambda r:(r['recording_msid'],r['artist_name'],r['track_name'],r['recording_mbid'] or ''))
    output_rows=[preview({**row,'sample_id':i}) for i,row in enumerate(rows)]
    if baseline:
        result['baseline_comparison']=compare_baseline(output_rows,json.loads(baseline.read_text(encoding='utf-8')))
    official=duckdb.connect(str(prepared/'experiment.duckdb'),read_only=True);official.execute('set threads=2')
    refs=sorted({r['resolved_mbid'] for r in rows if r['resolved_mbid']})
    expected={gid:(name,comment) for gid,name,comment in official.execute(
        'select gid,name,comment from raw_recording where gid in(select unnest(?))',[refs]).fetchall()}
    for row in rows:
        if row['resolved_mbid'] and expected.get(row['resolved_mbid'])!=(row['recording_title'],row['comment']):
            raise ValueError('official sample mismatch')
    official.close()
    result['sample_rows']=len(rows)
    result['unknown_reference_rows']=sum(r['resolved_mbid'] is None for r in rows)
    result['changed_text_rows']=sum(bool(r['changes']) for r in output_rows)
    result['change_counts']=dict(Counter(e['rule'] for r in output_rows for e in r['changes']))
    result['official_recordings_checked']=len(refs)
    result['groups']={mode:review_groups(output_rows,mode) for mode in MODES}
    result['group_verdict_counts']={m:dict(Counter(g['verdict'] for g in groups)) for m,groups in result['groups'].items()}
    result['source_after']=fingerprint(silver)
    if result['source_after']!=before or any(hashlib.sha256(Path(p).read_bytes()).hexdigest()!=v for p,v in hashes.items()):
        raise ValueError('input changed')
    result['source_unchanged']=True
    result['completed']=True
    (output/'rows.json').write_text(json.dumps(output_rows,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['# 표본 이름 전처리 전후','', '| 번호 | 원래 아티스트 | 비교용 아티스트 | 원래 곡명 | 승인 전처리 곡명 | NFKC 후보 | 라틴 전사 후보 |',
           '| --- | --- | --- | --- | --- | --- | --- |']
    def cell(value):
        text=str(value).replace('|','\\|').replace('\n','\\n').replace('\r','\\r')
        fence='`'*(1+max((len(s) for s in re.findall('`+',text)),default=0))
        return fence+' '+text+' '+fence
    for r in output_rows:
        vals=[r['original']['sample_id'],r['original']['artist_name'],r['approved']['artist'],
              r['original']['track_name'],r['approved']['title'],r['review_only']['nfkc']['title'],r['review_only']['latin_fold']['title']]
        lines.append('| '+' | '.join(cell(v) for v in vals)+' |')
    (output/'before-after.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k not in ('groups','registry')},ensure_ascii=False))
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for n in ('pool','review','prepared','silver','output'):
        p.add_argument('--'+n,type=Path,required=True)
    p.add_argument('--baseline',type=Path)
    a=p.parse_args();run(a.pool,a.review,a.prepared,a.silver,a.output,a.baseline)
