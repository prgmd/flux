import json
from collections import Counter,defaultdict
from dataclasses import asdict
from itertools import combinations
from pathlib import Path

from flux.review.evidence import build_evidence
from flux.review.journal import ReviewJournal,make_candidate,observation_id
from flux.review.sandbox import eligible_msid_mappings,validate_monthly_sandbox
from flux.silver.accent_review import previous_text
from flux.silver.name_candidates import normalize_text


def write_json(path,value):
    Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')


def _dicts(c,query,parameters):
    result=c.execute(query,parameters)
    keys=[column[0] for column in result.description]
    return [dict(zip(keys,row)) for row in result.fetchall()]


def review_sample(c,sample,preview,source_state,source_paths,output):
    output=Path(output).resolve()
    for source in source_paths:
        path=Path(source).resolve()
        if output==path or output in path.parents or path in output.parents:
            raise ValueError('overlapping review output')
    output.mkdir(parents=True,exist_ok=False)
    originals=[row['original'] for row in preview]
    gids=sorted(sample['official_recordings'])
    official={r['gid']:r for r in _dicts(c,'select id,gid,name,comment,artist_credit,video from raw_recording where gid in(select unnest(?))',[gids])}
    for gid,old in sample['official_recordings'].items():
        row=official.get(gid)
        if not row or (row['name'],row['comment'],row['artist_credit'])!=(old['title'],old['comment'],old['artist_credit']):
            raise ValueError('official recording changed: '+gid)
    credit_ids=sorted({r['artist_credit'] for r in official.values()})
    credits=defaultdict(list)
    query='''select n.artist_credit,n.position,a.gid as artist_mbid,a.name,a.comment,a.type
        from raw_artist_credit_name n join raw_artist a on n.artist=a.id
        where n.artist_credit in(select unnest(?)) order by n.artist_credit,cast(n.position as integer)'''
    for row in _dicts(c,query,[credit_ids]):
        credits[row.pop('artist_credit')].append(row)
    for credit in credit_ids:
        if credits[credit]!=sample['official_credits'][credit]:
            raise ValueError('official artist credit changed: '+credit)
    old_gids=sorted({r['recording_mbid'] for r in originals if r.get('recording_mbid') and r.get('recording_mbid')!=r.get('resolved_mbid')})
    redirects={r['gid']:r for r in _dicts(c,'select gid,new_id from raw_recording_gid_redirect where gid in(select unnest(?))',[old_gids])}
    msids=sorted({r['recording_msid'] for r in originals})
    metadata=_dicts(c,'select distinct recording_msid,artist_name,track_name,recording_mbid,artist_mbids,mbid_source from metadata where recording_msid in(select unnest(?))',[msids])
    metadata_ids={observation_id(row) for row in metadata}
    if any(observation_id(row) not in metadata_ids for row in originals):
        raise ValueError('sample observation absent from current metadata')
    resolutions=dict(c.execute('select original_mbid,resolved_mbid from resolution where original_mbid in(select unnest(?))',
        [sorted({r['recording_mbid'] for r in metadata if r['recording_mbid']})]).fetchall())
    known=defaultdict(set)
    for row in metadata:
        if resolved:=resolutions.get(row['recording_mbid']):known[row['recording_msid']].add(resolved)
    facts={};proofs={};fact_rows=[]
    for row in originals:
        gid=resolutions.get(row['recording_mbid']) if row['recording_mbid'] else None
        if gid!=row.get('resolved_mbid'):
            raise ValueError('sample resolution changed')
        record=official.get(gid)
        fact,proof=build_evidence(row,record,credits.get(record['artist_credit'],[]) if record else [],
            snapshot_id=source_state['official_snapshot'],evidence_ref='facts.json#'+observation_id(row),
            redirect=redirects.get(row['recording_mbid']),conflict=len(known[row['recording_msid']])>1)
        oid=observation_id(row)
        facts[oid]=fact;proofs[oid]=proof
        fact_rows.append({'observation_id':oid,'original':row,'fact':asdict(fact),'proof':proof})
    write_json(output/'facts.json',fact_rows)
    write_json(output/'metadata_scope.json',metadata)
    journal=ReviewJournal(output)
    chosen={}
    for case in sample['selected_cases']:
        buckets=defaultdict(list)
        for left,right in combinations(case['rows'],2):
            if left['recording_msid']==right['recording_msid']:continue
            if tuple(previous_text(left[k]) for k in ('artist_name','track_name'))==tuple(previous_text(right[k]) for k in ('artist_name','track_name')):continue
            if tuple(normalize_text(left[k]) for k in ('artist_name','track_name'))!=tuple(normalize_text(right[k]) for k in ('artist_name','track_name')):continue
            ids=[observation_id(left),observation_id(right)]
            candidate=make_candidate([left,right],[facts[oid] for oid in ids],source_state,{oid:proofs[oid] for oid in ids})
            buckets[candidate['assessment']['status']].append(candidate)
        for candidates in buckets.values():
            for candidate in sorted(candidates,key=lambda row:row['candidate_id'])[:3]:
                chosen[candidate['candidate_id']]=candidate
    decisions=[];reports=[];revision_done=False
    for candidate in sorted(chosen.values(),key=lambda row:row['candidate_id']):
        eid=journal.register(candidate)
        assessment=candidate['assessment']
        status={'eligible':'approved','keep_separate':'keep_separate'}.get(assessment['status'],'hold')
        predecessor=None
        if status=='approved' and not revision_done:
            initial=journal.decide(eid,status='hold',reason_code='pending_evidence_review',
                note='공식 근거와 원문 및 MSID 전체 범위를 대조하기 전의 초기 보류.',reviewer_kind='agent',reviewer_id='flux-sample-review')
            predecessor=initial['decision_id'];decisions.append(initial);revision_done=True
        note={'approved':'공식 ID와 버전 근거 일치. 실험 사본에만 사용하는 기계 검수이며 사람 승인은 미완료.',
              'keep_separate':'공식 녹음 또는 아티스트 및 버전이 다르므로 분리 유지.',
              'hold':'근거가 부족하거나 충돌함. 미확인 원문에 MBID를 상속하지 않고 추가 검수 대기.'}[status]
        decision=journal.decide(eid,status=status,reason_code=assessment['reason'],note=note,
            reviewer_kind='agent',reviewer_id='flux-sample-review',supersedes=predecessor)
        decisions.append(decision)
        reports.append({'candidate_id':candidate['candidate_id'],'evidence_id':eid,'status':status,'reason':assessment['reason'],
                        'names':[[r['artist_name'],r['track_name']] for r in candidate['observations']],
                        'msids':[r['recording_msid'] for r in candidate['observations']],
                        'resolved_mbids':[r['resolved_recording_mbid'] for r in candidate['facts']]})
    production=journal.export_approved(source_state,mode='production')
    sandbox=journal.export_approved(source_state,mode='sandbox')
    write_json(output/'production_export.json',production)
    write_json(output/'sandbox_export.json',sandbox)
    scope_msids={r['source_msid'] for r in sandbox['mappings']}
    scope=eligible_msid_mappings([r for r in metadata if r['recording_msid'] in scope_msids],sandbox['mappings'])
    monthly=validate_monthly_sandbox(c,source_paths,sorted(scope_msids),scope['mappings'],output/'monthly')
    result={'production_applied':False,'full_corpus_applied':False,'source_state':source_state,
            'sample_rows':len(originals),'metadata_scope_rows':len(metadata),'candidate_pairs':len(reports),
            'latest_decisions':dict(Counter(r['status'] for r in reports)),'decision_events':len(decisions),
            'human_decisions':0,'production_approved_candidates':production['approved_candidates'],
            'sandbox_approved_candidates':sandbox['approved_candidates'],'observation_mappings':len(sandbox['mappings']),
            'msid_scope':scope,'monthly_validation':monthly,'cases':reports}
    write_json(output/'review.json',result)
    lines=['# 표본 검수 결과','', '모든 판단은 agent의 실험용 검수다. 사람 승인과 운영 적용은 완료하지 않았다.','']
    for row in reports:
        lines.extend(['## '+row['candidate_id'], '',json.dumps(row['names'],ensure_ascii=False), '',
                      row['status']+' / '+row['reason'], '', '근거: '+row['evidence_id'], ''])
    (output/'review.md').write_text('\n'.join(lines),encoding='utf-8')
    return result
