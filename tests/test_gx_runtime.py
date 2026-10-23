from copy import deepcopy
import json

import pytest

pytest.importorskip('great_expectations')

from test_quality_contract import dataset
from flux.validation.gx_pilot import validate
from flux.validation.quality_gate import promote

pytestmark=pytest.mark.gx


def test_real_gx_matches_contract_and_writes_local_docs(tmp_path):
    result=validate(dataset(),tmp_path/'ok')
    assert result['completed'] is True,result.get('gx')
    assert result['gx']['passed'] is True
    assert len(result['gx']['checks'])==3
    assert result['production_applied'] is False and result['promotion_ready'] is False
    assert list((tmp_path/'ok').rglob('index.html'))
    native=json.loads((tmp_path/'ok/gx-result.json').read_text())
    assert all(not check['result'] for validation in native['validation_results'] for check in validation['results'])
    promote(tmp_path/'ok',tmp_path/'current.json',sandbox_root=tmp_path)


@pytest.mark.parametrize('fault',['duplicate','blank_key','numeric_key','zero','fraction','boolean','bad_month','wrong_rank'])
def test_actual_gx_or_independent_contract_blocks_fault_and_keeps_old_pointer(tmp_path,fault):
    data=dataset()
    if fault=='duplicate':data['monthly'].append(deepcopy(data['monthly'][0]))
    elif fault=='blank_key':data['monthly'][0]['user_name']=' '
    elif fault=='numeric_key':data['monthly'][0]['user_name']=123
    elif fault=='zero':data['monthly'][0]['play_count']=0
    elif fault=='fraction':data['monthly'][0]['play_count']=1.5
    elif fault=='boolean':data['monthly'][0]['play_count']=True
    elif fault=='bad_month':data['monthly'][0]['listened_month']='2026-13'
    else:data['marts']['year']['song_metrics'][0]['listener_rank']=999
    pointer=tmp_path/'current.json';pointer.write_bytes(b'previous snapshot')
    result=validate(data,tmp_path/'bad')
    assert result['completed'] is False
    assert result['publishable'] is False
    assert result['gx']['completed'] is True,result['gx']
    assert 'error' not in result['gx']
    with pytest.raises(ValueError):promote(tmp_path/'bad',pointer,sandbox_root=tmp_path)
    assert pointer.read_bytes()==b'previous snapshot'
    assert result['gx']['passed'] is (fault=='wrong_rank')
    contract={row['rule_id']:row for row in result['contract']['checks']}
    assert all((row['success'] is (contract[row['rule_id']]['status']=='PASS'))
               for row in result['gx']['checks'] if contract[row['rule_id']]['status']!='SKIP')


def test_gx_engine_exception_is_recorded_and_cannot_complete(tmp_path,monkeypatch):
    import great_expectations as gx
    def fail(**kwargs):raise RuntimeError('injected engine failure')
    monkeypatch.setattr(gx,'get_context',fail)
    result=validate(dataset(),tmp_path/'error')
    assert result['completed'] is False and result['gx']['error']=='RuntimeError'


def test_missing_data_docs_cannot_be_reported_as_completed(tmp_path,monkeypatch):
    from great_expectations.checkpoint import UpdateDataDocsAction
    monkeypatch.setattr(UpdateDataDocsAction,'run',lambda *args,**kwargs:{})
    result=validate(dataset(),tmp_path/'no-docs')
    assert result['completed'] is False
    assert result['gx']['data_docs_generated'] is False
