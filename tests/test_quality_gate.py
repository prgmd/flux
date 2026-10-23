import json
from copy import deepcopy

import pytest

from flux.validation.quality_gate import complete,ownership,promote
from flux.validation.quality_contract import registry


def passing():
    return {'evaluation_completed':True,'quality_passed':True,'checks':[dict(rule_id=row['rule_id'],severity=row['severity'],status='PASS') for row in registry()['rules']]}


def gx_pass():
    return {'completed':True,'passed':True,'checks':[{'rule_id':name,'success':True} for name in ['DQ003','DQ004','DQ005']]}


def test_ownership_covers_all_contract_rules_without_moving_row_cleaning():
    rows=ownership()
    assert len(rows)==17 and len({row['rule_id'] for row in rows})==17
    assert next(row for row in rows if row['rule_id']=='DQ008')['engine']=='spark-python'
    assert next(row for row in rows if row['rule_id']=='DQ004')['engine']=='gx'


@pytest.mark.parametrize('checks',[[],[{'rule_id':'DQ001','severity':'error','status':'SKIP'}],
    [{'rule_id':'DQ001','severity':'error','status':'FAIL'}]])
def test_required_failure_or_skipped_check_cannot_complete(tmp_path,checks):
    contract=passing();contract['checks']=checks
    result=complete(contract,gx_pass(),artifacts_verified=True)
    assert result['completed'] is False and result['publishable'] is False


def test_warning_is_reported_but_gx_exception_or_integrity_failure_blocks_completion():
    contract=passing();next(row for row in contract['checks'] if row['rule_id']=='DQ016')['status']='WARN'
    gx=gx_pass()
    assert complete(contract,gx,artifacts_verified=True)['completed'] is True
    for changes in [{'completed':False},{'passed':False},{'error':'engine unavailable'}]:
        bad=deepcopy(gx);bad.update(changes)
        assert complete(contract,bad,artifacts_verified=True)['completed'] is False
    assert complete(contract,gx,artifacts_verified=False)['completed'] is False


def test_failed_or_tampered_candidate_preserves_previous_pointer(tmp_path):
    old=tmp_path/'old';old.mkdir();new=tmp_path/'new';new.mkdir()
    (new/'mart.bin').write_bytes(b'correct')
    import hashlib
    digest=hashlib.sha256(b'correct').hexdigest()
    pointer=tmp_path/'current.json';pointer.write_text(json.dumps({'snapshot':str(old)}))
    before=pointer.read_bytes()
    result={'completed':False,'publishable':False,'artifacts':{'mart.bin':digest}}
    (new/'result.json').write_text(json.dumps(result))
    with pytest.raises(ValueError):promote(new,pointer,sandbox_root=tmp_path)
    assert pointer.read_bytes()==before
    result.update(completed=True,publishable=True)
    (new/'result.json').write_text(json.dumps(result));(new/'mart.bin').write_bytes(b'changed')
    with pytest.raises(ValueError):promote(new,pointer,sandbox_root=tmp_path)
    assert pointer.read_bytes()==before


def test_verified_candidate_can_replace_only_explicit_sandbox_pointer(tmp_path):
    new=tmp_path/'new';new.mkdir();(new/'mart.bin').write_bytes(b'correct')
    import hashlib
    result={'completed':True,'publishable':True,'contract':passing(),'gx':gx_pass(),'artifacts':{'mart.bin':hashlib.sha256(b'correct').hexdigest()}}
    (new/'result.json').write_text(json.dumps(result))
    pointer=tmp_path/'current.json'
    with pytest.raises(ValueError):promote(new,pointer,sandbox_root=tmp_path/'unrelated')
    promote(new,pointer,sandbox_root=tmp_path)
    assert json.loads(pointer.read_text())['snapshot']==str(new.resolve())
    assert result['completed'] is True


def test_missing_or_duplicated_engine_results_and_unsafe_artifacts_are_rejected(tmp_path):
    gx=gx_pass();gx['checks'].pop()
    assert complete(passing(),gx,artifacts_verified=True)['completed'] is False
    gx=gx_pass();gx['checks'].append(deepcopy(gx['checks'][0]))
    assert complete(passing(),gx,artifacts_verified=True)['completed'] is False
    root=tmp_path/'candidate';root.mkdir()
    (root/'result.json').write_text(json.dumps({'completed':True,'publishable':True,'artifacts':{'../escape':'anything'}}))
    with pytest.raises(ValueError):promote(root,tmp_path/'current.json',sandbox_root=tmp_path)
