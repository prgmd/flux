import argparse
import hashlib
import json
import unicodedata as ud
from collections import Counter
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.silver.accent_review import accent_groups,analyze_accent_group,previous_text,representative_rows
from flux.silver.name_candidates import POLICY_VERSION,normalize_text
from flux.silver.name_character_catalog import policy
from flux.silver.name_preview import preview


ARTIST_FILTERS = ['bjork','sigur ros','motorhead','tiesto','jose gonzalez','celine dion','zoe','cafe tacvba',
                  'rosalia','boa','maneskin','mylene farmer','edith piaf','roisin murphy',"l'imperatrice",
                  'olafur arnalds','antonin dvorak','mø','børns','cafe del mar','stromae','cœur de pirate']
CASES = [('celine dion','parler a mon pere'),("l'imperatrice",'deja-vue'),('mylene farmer',"a l'ombre"),
         ('rosalia','yo x ti, tu x mi'),('roisin murphy','let me know'),('motorhead','asylum choir'),
         ('bjork','joga'),('sigur ros','glosoli'),('zoe','via lactea'),('tiesto','adagio for strings'),
         ('jose gonzalez','heartbeats'),('boa','duvet'),('motorhead','ace of spades'),
         ('stromae','ta fete (j.a.c.k remix)'),('cafe del mar','romance anonimo'),('sigur ros','fljotavik')]
CONTROLS = ('mø','børns')
ARTIST_BOUNDARIES = ('bôa','boa')


def fingerprint(root):
    return {name:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(root/name))).hexdigest()
            for name in ('recording_metadata','monthly_user_recordings')}


def run(prepared,silver,output):
    prepared,silver,output=(p.resolve() for p in (prepared,silver,output))
    if any(output==p or p in output.parents or output in p.parents for p in (prepared,silver)):
        raise ValueError('overlapping output')
    before=fingerprint(silver)
    prior=json.loads((prepared/'result.json').read_text(encoding='utf-8'))
    if before!=prior['source_before']['silver']:
        raise ValueError('prepared source mismatch')
    if ud.unidata_version!=policy()['unicode_version']:
        raise ValueError('Unicode version mismatch')
    output.mkdir(parents=True,exist_ok=False)
    result={'completed':False,'production_applied':False,'full_corpus_preprocessed':False,
            'scope':'purpose-selected artist metadata; reference conflicts are not an audio error rate',
            'artist_filters':ARTIST_FILTERS,'source_before':before,
            'candidate_policy_version':POLICY_VERSION,'registry':policy()}
    report=output/'result.json'
    report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    c=duckdb.connect(str(prepared/'experiment.duckdb'),read_only=True)
    c.execute('set threads=2');c.execute("set memory_limit='4GB'")
    c.execute('create temp table targets as select distinct * from metadata where lower(strip_accents(trim(artist_name))) in (select unnest(?))',[ARTIST_FILTERS])
    count=c.execute('select count(*) from targets').fetchone()[0]
    if not 0<count<=250000:
        raise ValueError('sample pool size outside declared limit')
    print('targeted metadata rows: '+str(count),flush=True)
    pool=output/'pool.parquet'
    c.execute(f"""copy (select m.*,x.resolved_mbid,r.recording_title,r.comment,r.artist_credit,a.credit_display
        from targets m left join resolution x on m.recording_mbid=x.original_mbid
        left join recordings r on x.resolved_mbid=r.recording_mbid
        left join credit_displays a using(artist_credit)) to '{pool.as_posix()}' (format parquet)""")
    reader=duckdb.connect()
    cursor=reader.execute('select * from read_parquet(?)',[str(pool)])
    columns=[d[0] for d in cursor.description]
    rows=[dict(zip(columns,row)) for row in cursor.fetchall()]
    reader.close()
    groups=accent_groups(rows)
    index={tuple(g['key']):g for g in groups}
    selected=[]
    for key in CASES:
        if key not in index:
            raise ValueError('missing case: '+str(key))
        group=index[key]
        sample=analyze_accent_group(representative_rows(group['rows']))
        sample['pool_group_rows']=len(group['rows'])
        if sample['verdict']!=group['verdict']:
            raise ValueError('representatives lost reference evidence')
        selected.append(sample)
    ordered=sorted(rows,key=lambda r:(r['recording_msid'],r.get('recording_mbid') or ''))
    controls=[]
    for artist in CONTROLS:
        choices=[r for r in ordered if normalize_text(r['artist_name'])==artist and r['resolved_mbid']]
        if not choices:raise ValueError('missing control: '+artist)
        controls.append(choices[0])
    boundaries=[]
    for artist in ARTIST_BOUNDARIES:
        choices=[r for r in ordered if r['resolved_mbid'] and r['credit_display'] and previous_text(r['credit_display'])==artist]
        if not choices:raise ValueError('missing artist boundary: '+artist)
        boundaries.append(choices[0])
    sample_rows=[r for case in selected for r in case['rows']]+controls+boundaries
    refs=sorted({r['resolved_mbid'] for r in sample_rows if r['resolved_mbid']})
    raw=c.execute('select gid,name,comment,artist_credit from raw_recording where gid in (select unnest(?))',[refs]).fetchall()
    official={gid:{'title':name,'comment':comment,'artist_credit':credit} for gid,name,comment,credit in raw}
    if set(official)!=set(refs):raise ValueError('missing official recordings')
    for row in sample_rows:
        if row['resolved_mbid']:
            expected=official[row['resolved_mbid']]
            if (row['recording_title'],row['comment'],row['artist_credit'])!=(expected['title'],expected['comment'],expected['artist_credit']):
                raise ValueError('official recording mismatch')
    credit_ids=sorted({r['artist_credit'] for r in sample_rows if r['artist_credit']})
    credited=c.execute('''select n.artist_credit,n.position,a.gid,a.name,a.comment,a.type
        from raw_artist_credit_name n join raw_artist a on n.artist=a.id
        where n.artist_credit in (select unnest(?)) order by n.artist_credit,try_cast(n.position as integer)''',[credit_ids]).fetchall()
    credits={credit:[] for credit in credit_ids}
    for credit,position,gid,name,comment,kind in credited:
        credits[credit].append({'position':position,'artist_mbid':gid,'name':name,'comment':comment,'type':kind})
    if any(not values for values in credits.values()):raise ValueError('missing official artist credit')
    c.close()
    result.update({'targeted_metadata_rows':count,'pool_rows':len(rows),'accent_candidate_groups':len(groups),
                   'pool_group_verdict_counts':dict(Counter(g['verdict'] for g in groups)),
                   'selected_cases':selected,'selected_case_verdict_counts':dict(Counter(g['verdict'] for g in selected)),
                   'sample_rows':len(sample_rows),'sample_cases':len(selected),'controls':controls,'artist_boundaries':boundaries,
                   'official_recordings_checked':len(refs),'official_recordings':official,'official_credits':credits,
                   'pool_sha256':hashlib.sha256(pool.read_bytes()).hexdigest(),'source_after':fingerprint(silver)})
    if result['source_after']!=before:raise ValueError('source changed')
    previews=[preview(row) for row in sample_rows]
    (output/'rows.json').write_text(json.dumps(previews,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['# 악센트 표본 확대 검수','', '| 원래 아티스트 | 원래 곡명 | 비교용 아티스트 | 비교용 곡명 | 녹음 MBID | 공식 녹음 설명 |',
           '| --- | --- | --- | --- | --- | --- |']
    def cell(value):
        return str(value or '').replace('|','\\|').replace('\n',' ').replace('\r',' ')
    for row in previews:
        original=row['original'];approved=row['approved']
        lines.append('| '+' | '.join(cell(v) for v in (original['artist_name'],original['track_name'],approved['artist'],approved['title'],original['resolved_mbid'],original['comment']))+' |')
    (output/'before-after.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    result['completed']=True;result['source_unchanged']=True
    report.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps({k:result[k] for k in ('completed','targeted_metadata_rows','accent_candidate_groups','pool_group_verdict_counts','sample_cases','sample_rows','selected_case_verdict_counts','official_recordings_checked','source_unchanged')},ensure_ascii=False),flush=True)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for name in ('prepared','silver','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();run(args.prepared,args.silver,args.output)
