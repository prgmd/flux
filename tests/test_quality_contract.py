from copy import deepcopy

import pytest

from flux.validation.oracle import python_marts
from flux.validation.quality_contract import evaluate,registry,run


def record(played):
    return {'document':{'user_name':'sample','listened_at':1,'track_metadata':{'additional_info':{'ms_played':played}}},'ingested_at':2}


def dataset():
    monthly=[{'user_name':u,'recording_msid':m,'listened_month':month,'play_count':n} for u,m,month,n in
             [('u','one','2026-01',2),('u','two','2026-01',3),('v','one','2026-01',1),
              ('u','one','2026-02',4),('v','fast','2026-01',7),('w','unknown','2026-01',1)]]
    dimension=[{'recording_msid':m,'song_key':key,'variant':variant} for m,key,variant in
               [('one','r','unmarked'),('two','r','unmarked'),('fast','fast','sped_up+remix'),('unknown','unknown','unmarked')]]
    labels=[{'song_key':key,'song_name':key,'observed_artist':'Artist','variant':variant,'identity_status':status,'source_msids':n}
            for key,variant,status,n in [('r','unmarked','matched_candidate',2),('fast','sped_up+remix','no_mbid',1),('unknown','unmarked','no_mbid',1)]]
    credits=[{'song_key':'r','artist_mbid':aid,'artist_name':aid} for aid in ['main','guest']]
    return {'integrity':True,'context':{'listener_key':'user_name','timezone':'UTC','minimum_ms_played':30000,
             'scope_id':'fixed-sample','r9_reference_only':True},'monthly':monthly,'dimension':dimension,
            'labels':labels,'credits':credits,'metadata':[{'recording_mbid':None}],
            'marts':{p:python_marts(monthly,dimension,labels,credits,p) for p in ['month','year']},
            'valid_details':[record(None),record('30001')],
            'quarantine':[dict(record(29999),reason='재생 시간이 30초 미만')],
            'raw_probe':[record(None),record('30001')],
            'r9_export':{'approved_candidates':0,'mappings':[]},
            'dump_summary':{'gaps':[],'overlaps':[],'new_input_sequence_ready':False}}


def checks(result):
    return {row['rule_id']:row for row in result['checks']}


def test_registry_and_valid_sample_allow_missing_ids_and_multi_artist_credit():
    rules=registry()
    assert len({r['rule_id'] for r in rules['rules']})==len(rules['rules'])
    assert all({'rule_id','severity','stage','target','owner','scope','null_policy','expectation','check'}<=r.keys() for r in rules['rules'])
    data=dataset();result=evaluate(data)
    assert result['quality_passed'] is True and result['promotion_ready'] is False
    assert checks(result)['DQ007']['status']=='PASS'
    assert sum(r['listens'] for r in data['marts']['year']['artist_metrics'])>sum(r['play_count'] for r in data['monthly'])


@pytest.mark.parametrize('value',[None,0,-1,1.5,True])
def test_invalid_play_counts_fail_without_crashing_other_checks(value):
    data=dataset();data['monthly'][0]['play_count']=value
    result=evaluate(data)
    assert checks(result)['DQ005']['status']=='FAIL'
    assert result['quality_passed'] is False


def test_required_fields_and_duplicate_monthly_keys_fail():
    data=dataset();data['monthly'][0]['user_name']=' '
    assert checks(evaluate(data))['DQ003']['status']=='FAIL'
    data=dataset();data['monthly'].append(deepcopy(data['monthly'][0]))
    result=evaluate(data)
    assert checks(result)['DQ004']['status']=='FAIL'
    assert checks(result)['DQ012']['status']=='SKIP'


@pytest.mark.parametrize('field,value',[('listeners',999),('top_user_share',1.1),('concentration_hhi',float('nan'))])
def test_metric_bounds_fail(field,value):
    data=dataset();data['marts']['month']['song_metrics'][0][field]=value
    assert checks(evaluate(data))['DQ011']['status']=='FAIL'


def test_counts_ranks_and_annual_distinct_are_checked_independently():
    data=dataset();data['marts']['month']['song_metrics'][0]['listens']+=1
    assert checks(evaluate(data))['DQ010']['status']=='FAIL'
    data=dataset();data['marts']['month']['song_metrics'][0]['listener_rank']=99
    assert checks(evaluate(data))['DQ012']['status']=='FAIL'
    data=dataset();data['marts']['year']['song_metrics'][0]['listeners']+=1
    assert checks(evaluate(data))['DQ012']['status']=='FAIL'


def test_short_valid_listen_fails_but_missing_duration_and_30000_pass():
    data=dataset();data['valid_details']=[record(None),record('30000'),record('30001')]
    assert checks(evaluate(data))['DQ008']['status']=='PASS'
    data['valid_details'].append(record('29999'))
    assert checks(evaluate(data))['DQ008']['status']=='FAIL'
    data=dataset();data['valid_details']=None
    assert checks(evaluate(data))['DQ008']['status']=='SKIP'
    data['valid_details']=[]
    assert checks(evaluate(data))['DQ008']['status']=='SKIP'


def test_quarantine_reason_and_version_partition_cannot_be_silently_changed():
    data=dataset();data['quarantine'][0]['reason']='다른 사유'
    assert checks(evaluate(data))['DQ009']['status']=='FAIL'
    data=dataset();data['labels'][1]['variant']='unmarked'
    assert checks(evaluate(data))['DQ013']['status']=='FAIL'


def test_unapproved_mapping_and_incompatible_context_fail():
    data=dataset();data['r9_export']={'approved_candidates':1,'mappings':[{'reviewer_kind':'agent'}]}
    assert checks(evaluate(data))['DQ014']['status']=='FAIL'
    data=dataset();data['context']['listener_key']='user_id'
    assert checks(evaluate(data))['DQ002']['status']=='FAIL'
    data=dataset();data['r9_export']['approved_candidates']=False
    assert checks(evaluate(data))['DQ014']['status']=='FAIL'
    data=dataset();data['context']['scope_id']=None
    assert checks(evaluate(data))['DQ002']['status']=='FAIL'


def test_unreadable_duration_and_collection_gaps_warn_without_deleting_rows():
    data=dataset();data['raw_probe'].append(record('unknown'))
    data['dump_summary']['gaps']=[{'missing':'period'}]
    before=deepcopy(data);result=evaluate(data)
    assert data==before
    assert checks(result)['DQ015']['status']=='WARN'
    assert checks(result)['DQ016']['status']=='WARN'
    assert checks(result)['DQ017']['status']=='WARN'
    assert result['quality_passed'] is True


def test_profile_comparison_requires_same_scope_and_warns_on_change():
    data=dataset();first=evaluate(data)
    assert checks(first)['DQ017']['status']=='WARN'
    reference=first['profile']
    assert checks(evaluate(data,reference_profile=reference))['DQ017']['status']=='PASS'
    other=deepcopy(reference);other['scope_id']='different'
    assert checks(evaluate(data,reference_profile=other))['DQ017']['status']=='WARN'
    changed=deepcopy(reference);changed['values']['monthly_rows']+=1
    result=evaluate(data,reference_profile=changed)
    assert checks(result)['DQ017']['status']=='WARN' and result['quality_passed'] is True


def test_missing_baseline_generates_failed_report_and_never_overwrites_source(tmp_path):
    baseline=tmp_path/'baseline';baseline.mkdir()
    result=run(baseline,tmp_path/'report')
    assert result['quality_passed'] is False and result['evaluation_completed'] is True
    assert checks(result)['DQ001']['status']=='FAIL'
    assert (tmp_path/'report/result.json').exists()
    with pytest.raises(ValueError):run(baseline,baseline/'report')
    with pytest.raises(FileExistsError):run(baseline,tmp_path/'report')


def test_end_of_run_integrity_failure_is_reflected_in_rule_summary(tmp_path,monkeypatch):
    import flux.validation.quality_contract as module

    baseline=tmp_path/'baseline';baseline.mkdir();(baseline/'result.json').write_text('{}')
    monkeypatch.setattr(module,'load_baseline',lambda root:dataset())
    def changed(root):raise ValueError('changed')
    monkeypatch.setattr(module,'verify_bundle',changed)
    result=run(baseline,tmp_path/'report')
    assert result['quality_passed'] is False
    assert result['baseline_unchanged'] is False
    assert checks(result)['DQ001']['status']=='FAIL'
    assert result['summary']['FAIL']>=1


def test_fault_drill_detects_corruption_and_preserves_its_input():
    from flux.validation.quality_drill import drill

    data=dataset();before=deepcopy(data)
    result=drill(data)
    assert data==before
    assert result['drill_passed'] is True
    assert result['scenarios_checked']==16
    assert all(r['expectation_met'] for r in result['scenarios'])
    assert all('user_name' not in r for r in result['scenarios'])


def test_fault_drill_does_not_claim_success_when_checker_misses_failures(monkeypatch):
    import flux.validation.quality_drill as module

    baseline=evaluate(dataset())
    monkeypatch.setattr(module,'evaluate',lambda *args,**kwargs:deepcopy(baseline))
    result=module.drill(dataset())
    assert result['drill_passed'] is False
    assert any(not r['expectation_met'] for r in result['scenarios'])


def test_null_song_key_cannot_pass_as_a_resolved_reference():
    data=dataset()
    for row in data['dimension']:
        if row['song_key']=='r':row['song_key']=None
    data['labels'][0]['song_key']=None
    for row in data['credits']:row['song_key']=None
    assert checks(evaluate(data))['DQ006']['status']=='FAIL'


def test_unreadable_parquet_generates_failed_report(tmp_path,monkeypatch):
    import duckdb
    import flux.validation.quality_contract as module

    baseline=tmp_path/'baseline';baseline.mkdir();(baseline/'result.json').write_text('{}')
    def unreadable(root):raise duckdb.InvalidInputException('bad parquet')
    monkeypatch.setattr(module,'load_baseline',unreadable)
    result=run(baseline,tmp_path/'report')
    assert result['quality_passed'] is False
    assert checks(result)['DQ001']['status']=='FAIL'
