import json

import pytest

from flux.validation.full_gold_cdf import recovered_models, resume_full_cdf, verify_executed_sql


def proof(tmp_path):
    target=tmp_path/'target-build'
    target.mkdir()
    nodes={f'model.flux_gold.m{i}':dict(resource_type='model',name=f'm{i}',unique_id=f'model.flux_gold.m{i}') for i in range(24)}
    nodes.update({f'test.flux_gold.t{i}':dict(resource_type='test',name=f't{i}',unique_id=f'test.flux_gold.t{i}') for i in range(145)})
    (target/'manifest.json').write_text(json.dumps(dict(nodes=nodes)),encoding='utf-8')
    text='\n'.join(f'08:00:00  {i+1} of 169 OK created sql table model analytics.m{i} ... [OK in 0.50s]' for i in range(24))
    (tmp_path/'build.log').write_text(text,encoding='utf-8')
    return text


def test_recovery_identifies_completed_models(tmp_path):
    proof(tmp_path)
    nodes=recovered_models(tmp_path)
    assert len(nodes)==24 and all(node['status']=='success' for node in nodes)
    assert sum(node['execution_time'] for node in nodes)==12


def test_recovery_keeps_unfinished_models_for_a_new_build(tmp_path):
    text=proof(tmp_path)
    (tmp_path/'build.log').write_text(text.replace('OK created sql table model analytics.m23','START sql table model analytics.m23'),encoding='utf-8')
    nodes=recovered_models(tmp_path)
    assert len(nodes)==23 and 'model.flux_gold.m23' not in {node['unique_id'] for node in nodes}


def test_executed_sql_allows_dbt_newlines_but_rejects_changed_logic(tmp_path):
    path=tmp_path/'model.sql'
    path.write_text('select\n  count(*) as n\n',encoding='utf-8')
    node=dict(original_file_path='model.sql',package_name='flux_gold',raw_code='select\r\n  count(*) as n')
    verify_executed_sql(tmp_path,[node])
    path.write_text('select\n  sum(n) as n\n',encoding='utf-8')
    with pytest.raises(ValueError,match='checksum'):verify_executed_sql(tmp_path,[node])


@pytest.mark.parametrize('state',['completed','cursor','stage','prior'])
def test_recovery_rejects_wrong_completion_state_before_opening_data(tmp_path,state):
    report=dict(completed=state=='completed',stage='different' if state=='stage' else 'dbt full rebuild and tests')
    (tmp_path/'result.json').write_text(json.dumps(report),encoding='utf-8')
    if state=='cursor':(tmp_path/'cursor.json').write_text('{}',encoding='utf-8')
    if state=='prior':(tmp_path/'interrupted-result.json').write_text('{}',encoding='utf-8')
    with pytest.raises(ValueError):resume_full_cdf(tmp_path/'absent-source',tmp_path,tmp_path/'absent-project')


@pytest.mark.parametrize('mutation',['none','duplicate','unknown','error','manifest'])
def test_recovery_rejects_incomplete_or_untrusted_evidence(tmp_path,mutation):
    text=proof(tmp_path)
    if mutation=='none':text=''
    elif mutation=='duplicate':text+='\n'+text.splitlines()[0]
    elif mutation=='unknown':text=text.replace('analytics.m23','analytics.unknown')
    elif mutation=='error':text+='\nERROR encountered before interruption'
    else:(tmp_path/'target-build/manifest.json').write_text('{"nodes": {}}',encoding='utf-8')
    (tmp_path/'build.log').write_text(text,encoding='utf-8')
    with pytest.raises(ValueError):recovered_models(tmp_path)
