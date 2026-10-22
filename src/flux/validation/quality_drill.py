import json
from copy import deepcopy
from pathlib import Path

from flux.silver.quality import judge
from flux.validation.pinning import sha256,verify_bundle
from flux.validation.quality_contract import evaluate,load_baseline


def _played(item,value):
    item['document'].setdefault('track_metadata',{}).setdefault('additional_info',{})['ms_played']=value


def _mutate(data,name):
    if name=='blank_user':data['monthly'][0]['user_name']=' '
    elif name=='duplicate_key':data['monthly'].append(deepcopy(data['monthly'][0]))
    elif name=='zero_count':data['monthly'][0]['play_count']=0
    elif name=='fractional_count':data['monthly'][0]['play_count']=1.5
    elif name=='missing_dimension':data['dimension']=[r for r in data['dimension'] if r['recording_msid']!=data['monthly'][0]['recording_msid']]
    elif name=='wrong_total':data['marts']['month']['song_metrics'][0]['listens']+=1
    elif name=='invalid_share':data['marts']['month']['song_metrics'][0]['top_user_share']=1.1
    elif name=='wrong_rank':data['marts']['month']['song_metrics'][0]['listener_rank']=999999
    elif name=='annual_listener_overcount':data['marts']['year']['song_metrics'][0]['listeners']+=1
    elif name=='changed_version':data['labels'][0]['variant']='injected_wrong_version'
    elif name=='unapproved_mapping':data['r9_export']={'approved_candidates':1,'mappings':[{'reviewer_kind':'agent'}]}
    elif name=='wrong_quarantine_reason':data['quarantine'][0]['reason']='injected_wrong_reason'
    elif name=='invalid_duration':_played(data['raw_probe'][0],'unreadable')
    elif name=='wrong_listener_key':data['context']['listener_key']='user_id'
    elif name in ('short_valid_detail','accepted_duration_boundaries'):
        item=next(r for r in data['raw_probe'] if judge(r['document'],ingested_at=r['ingested_at']).ok)
        data['valid_details']=[]
        for value in ([29999] if name=='short_valid_detail' else [None,'30000','30001']):
            row=deepcopy(item);_played(row,value);data['valid_details'].append(row)
    else:raise ValueError('unknown drill scenario')


def drill(data):
    baseline=evaluate(data)
    scenarios=[]
    cases=[('blank_user','DQ003','FAIL'),('duplicate_key','DQ004','FAIL'),('zero_count','DQ005','FAIL'),
           ('fractional_count','DQ005','FAIL'),('missing_dimension','DQ006','FAIL'),('wrong_total','DQ010','FAIL'),
           ('invalid_share','DQ011','FAIL'),('wrong_rank','DQ012','FAIL'),('annual_listener_overcount','DQ012','FAIL'),
           ('changed_version','DQ013','FAIL'),('unapproved_mapping','DQ014','FAIL'),('wrong_quarantine_reason','DQ009','FAIL'),
           ('short_valid_detail','DQ008','FAIL'),('invalid_duration','DQ015','WARN'),
           ('accepted_duration_boundaries','DQ008','PASS'),('wrong_listener_key','DQ002','FAIL')]
    for name,rule,status in cases:
        changed=deepcopy(data);_mutate(changed,name);result=evaluate(changed)
        actual=next(r['status'] for r in result['checks'] if r['rule_id']==rule)
        expected_pass=status!='FAIL'
        scenarios.append({'scenario':name,'rule_id':rule,'expected_status':status,'actual_status':actual,
                          'quality_passed':result['quality_passed'],
                          'expectation_met':actual==status and result['quality_passed']==expected_pass})
    return {'drill_passed':baseline['quality_passed'] and all(r['expectation_met'] for r in scenarios),
            'baseline_quality_passed':baseline['quality_passed'],'scenarios_checked':len(scenarios),
            'scenarios':scenarios,'mutation_scope':'in-memory copies only','production_applied':False}


def run_drill(baseline,output):
    baseline,output=Path(baseline).resolve(),Path(output).resolve()
    if output==baseline or output in baseline.parents or baseline in output.parents:raise ValueError('overlapping report output')
    output.mkdir(parents=True,exist_ok=False)
    before=sha256(baseline/'result.json');result=drill(load_baseline(baseline))
    verify_bundle(baseline)
    result['baseline_unchanged']=before==sha256(baseline/'result.json')
    result['drill_passed']=result['drill_passed'] and result['baseline_unchanged']
    result['baseline_manifest_sha256']=before
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    return result
