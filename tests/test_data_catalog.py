import copy
import json
import shutil
import subprocess
from pathlib import Path

import pytest

from flux.common.execution import Journal
from flux.validation.data_catalog import build, collect
from flux.validation.pinning import sha256
from flux.validation.quality_contract import registry


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding='utf-8')


@pytest.fixture
def bundle(tmp_path, monkeypatch):
    monkeypatch.setenv('FLUX_EXECUTION_ROOT', str(tmp_path / 'catalog-journals'))
    root = tmp_path / 'dbt'
    baseline = tmp_path / 'baseline'
    write(baseline / 'source_context.json', {'tables': {
        'bronze': {'id': 'bronze', 'version': 3},
        'monthly_user_recordings': {'id': 'monthly', 'version': 5},
        'recording_metadata': {'id': 'metadata', 'version': 5}}})
    write(baseline / 'result.json', {'completed': True, 'artifacts': {
        'source_context.json': sha256(baseline / 'source_context.json')}})
    digest = sha256(baseline / 'result.json')
    context = {'listener_key': 'user_name', 'timezone': 'UTC', 'minimum_ms_played': 30000,
               'approved_mappings': 0, 'r9_reference_only': True,
               'source_state': {'source_id': 'bronze', 'version': 3}}
    write(root / 'inputs.json', {'baseline_path': str(baseline), 'baseline_sha256': digest,
                                'context': context, 'production_applied': False})
    project = root / 'project'
    write(project / 'dbt_project.yml', {'name': 'flux_gold'})
    project_hashes = {'dbt_project.yml': sha256(project / 'dbt_project.yml')}
    source_id = 'source.flux_gold.pinned.monthly'
    model_id = 'model.flux_gold.monthly_song_metrics'
    test_id = 'test.flux_gold.positive'
    columns = {'play_count': {'name': 'play_count', 'description': '반복 청취 횟수'}}
    source = {'unique_id': source_id, 'resource_type': 'source', 'name': 'monthly',
              'description': '월별 Silver 표본', 'columns': columns, 'meta': {'baseline_sha256': digest}}
    model = {'unique_id': model_id, 'resource_type': 'model', 'name': 'monthly_song_metrics',
             'description': '월별 곡 지표', 'columns': columns, 'checksum': {'checksum': 'a'},
             'config': {'meta': {'rule_ids': ['DQ010', 'DQ011']}, 'materialized': 'table'},
             'depends_on': {'nodes': [source_id]}}
    test = {'unique_id': test_id, 'resource_type': 'test', 'name': 'positive',
            'checksum': {'checksum': 'b'}, 'config': {}, 'depends_on': {'nodes': [model_id]}}
    manifest = {'metadata': {'invocation_id': 'docs', 'project_name': 'flux_gold'},
                'sources': {source_id: source}, 'nodes': {model_id: model, test_id: test}, 'macros': {}}
    catalog_entry = {'columns': {'play_count': {'name': 'play_count', 'type': 'BIGINT'}}}
    write(root / 'target-docs/manifest.json', manifest)
    write(root / 'target-docs/catalog.json', {'metadata': {'invocation_id': 'docs'},
          'nodes': {model_id: catalog_entry}, 'sources': {source_id: catalog_entry}})
    (root / 'target-docs/index.html').write_text('<html>dbt docs</html>', encoding='utf-8')
    for tag in ['build', 'rebuild']:
        other = copy.deepcopy(manifest)
        other['metadata']['invocation_id'] = tag
        write(root / f'target-{tag}/manifest.json', other)
        write(root / f'target-{tag}/run_results.json', {
            'metadata': {'invocation_id': tag}, 'results': [
                {'unique_id': model_id, 'status': 'success', 'execution_time': .2, 'timing': []},
                {'unique_id': test_id, 'status': 'pass', 'execution_time': .1, 'timing': []}]})
    comparison = {'source_rows': 2, 'plays': 7, 'tables_checked': 1, 'mismatches': 0,
                  'table_rows': {'monthly_song_metrics': 1}}
    result = {'completed': True, 'production_applied': False, 'project_sha256': project_hashes,
              'input_manifest_sha256': sha256(root / 'inputs.json'), 'final_comparison': comparison}
    write(root / 'result.json', result)
    checks = [{'rule_id': r['rule_id'], 'severity': r['severity'], 'status':
               'WARN' if r['rule_id'] in ['DQ015', 'DQ017'] else 'SKIP' if r['rule_id'] == 'DQ008' else 'PASS',
               'observed': {'private_sample': 'never-export-this-user'}} for r in registry()['rules']]
    contract = {'evaluation_completed': True, 'production_applied': False,
                'baseline_manifest_sha256': digest, 'policy_version': registry()['version'],
                'registry_sha256': sha256(Path('src/flux/validation/quality_rules.json')),
                'checks': checks, 'quality_passed': True,
                'profile': {'scope_id': digest, 'values': {'monthly_rows': 2, 'plays': 7}}}
    quality, gx = tmp_path / 'quality', tmp_path / 'gx'
    write(quality / 'result.json', contract)
    gx_contract = {k: contract[k] for k in ['evaluation_completed', 'policy_version', 'checks',
                                          'profile', 'quality_passed']}
    write(gx / 'result.json', {'completed': True, 'production_applied': False,
          'baseline_manifest_sha256': digest, 'monthly_rows': 2, 'contract': gx_contract,
          'gx': {'completed': True, 'passed': True, 'checks': [
              {'rule_id': 'DQ003', 'success': True, 'expectations': 8},
              {'rule_id': 'DQ004', 'success': True, 'expectations': 1},
              {'rule_id': 'DQ005', 'success': True, 'expectations': 3}]}})
    journal = Journal(tmp_path / 'executions', 'dbt-sample', {'root': str(root.resolve())})
    journal.inputs({'baseline_sha256': digest, 'input_manifest_sha256': result['input_manifest_sha256'],
                    'project_sha256': project_hashes, 'context': context})
    journal.checkpoint(root, {**result, 'stage': 'build'})
    journal.finish('succeeded')
    return root, quality, gx, journal.path


def mutate(path, change):
    value = json.loads(path.read_text(encoding='utf-8'))
    change(value)
    write(path, value)


def test_meaning_lineage_quality_and_execution_are_joined_without_raw_rows(bundle):
    data = collect(*bundle)
    model = data['nodes']['model.flux_gold.monthly_song_metrics']
    assert model['description'] == '월별 곡 지표'
    assert model['grain'] == ['period_start', 'song_key']
    assert model['columns'][0]['type'] == 'BIGINT'
    assert model['parents'] == ['source.flux_gold.pinned.monthly']
    assert model['dbt_checks'][0]['runs']['build']['status'] == 'pass'
    assert model['runs']['rebuild']['status'] == 'success'
    assert data['execution']['status'] == 'succeeded'
    assert data['execution']['stages'][0]['name'] == 'build'
    assert data['quality']['summary'] == {'PASS': 14, 'WARN': 2, 'SKIP': 1}
    assert data['rules']['DQ008']['contract_status'] == 'SKIP'
    assert data['rules']['DQ003']['gx']['expectations'] == 8
    assert data['rules']['DQ010']['gx'] is None
    assert data['production_applied'] is False and data['promotion_ready'] is False
    assert 'never-export-this-user' not in json.dumps(data)
    assert 'bronze' in data['external_lineage']['nodes']
    assert all(e['kind'] == 'declared' for e in data['external_lineage']['edges'])


@pytest.mark.parametrize('kind', ['quality-baseline', 'gx-baseline', 'quality-registry', 'quality-rows',
                                 'project', 'journal-input', 'journal-output', 'journal-unfinished',
                                 'docs-invocation', 'build-invocation', 'missing-result',
                                 'unknown-parent', 'cycle', 'catalog-column', 'model-code',
                                 'source-baseline', 'gx-contract-scope', 'gx-missing-check', 'source-context'])
def test_unrelated_or_inconsistent_evidence_cannot_be_attached(bundle, kind):
    root, quality, gx, journal = bundle
    if kind == 'quality-baseline':
        mutate(quality / 'result.json', lambda d: d.update(baseline_manifest_sha256='other'))
    elif kind == 'gx-baseline':
        mutate(gx / 'result.json', lambda d: d.update(baseline_manifest_sha256='other'))
    elif kind == 'quality-registry':
        mutate(quality / 'result.json', lambda d: d.update(registry_sha256='old'))
    elif kind == 'quality-rows':
        mutate(quality / 'result.json', lambda d: d['profile']['values'].update(monthly_rows=9))
    elif kind == 'project':
        (root / 'project/dbt_project.yml').write_text('changed', encoding='utf-8')
    elif kind.startswith('journal-'):
        import sqlite3
        with sqlite3.connect(journal) as c:
            if kind == 'journal-unfinished':
                c.execute("delete from events where kind='run_finished'")
            else:
                row = c.execute('select seq,data from events where kind=?',
                                ['inputs' if kind == 'journal-input' else 'run_started']).fetchone()
                data = json.loads(row[1])
                if kind == 'journal-input':
                    data['baseline_sha256'] = 'other'
                else:
                    data['parameters']['root'] = str(root.parent / 'wrong')
                c.execute('update events set data=? where seq=?', [json.dumps(data), row[0]])
    elif kind == 'docs-invocation':
        mutate(root / 'target-docs/catalog.json', lambda d: d['metadata'].update(invocation_id='other'))
    elif kind == 'build-invocation':
        mutate(root / 'target-build/run_results.json', lambda d: d['metadata'].update(invocation_id='other'))
    elif kind == 'missing-result':
        mutate(root / 'target-build/run_results.json', lambda d: d['results'].pop())
    elif kind == 'catalog-column':
        mutate(root / 'target-docs/catalog.json', lambda d: d['nodes'][
            'model.flux_gold.monthly_song_metrics']['columns'].clear())
    elif kind == 'source-baseline':
        mutate(root / 'target-docs/manifest.json', lambda d: d['sources'][
            'source.flux_gold.pinned.monthly']['meta'].update(baseline_sha256='other'))
    elif kind == 'gx-contract-scope':
        mutate(gx / 'result.json', lambda d: d['contract']['profile'].update(scope_id='other'))
    elif kind == 'gx-missing-check':
        mutate(gx / 'result.json', lambda d: d['gx']['checks'].pop())
    elif kind == 'source-context':
        mutate(root.parent / 'baseline/source_context.json', lambda d: d['tables']['bronze'].update(version=8))
    else:
        name = 'model.flux_gold.monthly_song_metrics'
        def change(d):
            node = d['nodes'][name]
            if kind == 'model-code':
                node['checksum']['checksum'] = 'different'
            else:
                node['depends_on']['nodes'] = [name if kind == 'cycle' else 'unknown']
        for tag in ['docs', 'build', 'rebuild'] if kind in ['unknown-parent', 'cycle'] else ['docs']:
            mutate(root / f'target-{tag}/manifest.json', change)
    with pytest.raises(ValueError):
        collect(*bundle)


def test_failed_and_warned_checks_remain_visible(bundle):
    root, quality, gx, _ = bundle
    mutate(root / 'target-rebuild/run_results.json', lambda d: d['results'][1].update(status='fail'))
    mutate(quality / 'result.json', lambda d: (d.update(quality_passed=False),
           d['checks'][10].update(status='FAIL')))
    data = collect(*bundle)
    assert data['rules']['DQ011']['contract_status'] == 'FAIL'
    assert data['nodes']['model.flux_gold.monthly_song_metrics']['dbt_checks'][0]['runs']['rebuild']['status'] == 'fail'
    assert data['quality']['summary']['FAIL'] == 1
    assert not data['promotion_ready']


def test_dbt_runtime_wrapper_macros_and_partial_column_docs_are_supported(bundle):
    root = bundle[0]
    for tag in ['build', 'rebuild']:
        mutate(root / f'target-{tag}/manifest.json', lambda d: d['nodes'][
            'test.flux_gold.positive']['depends_on'].update(macros=['macro.dbt.statement']))
    for tag in ['docs', 'build', 'rebuild']:
        mutate(root / f'target-{tag}/manifest.json', lambda d: d['nodes'][
            'model.flux_gold.monthly_song_metrics']['columns'].clear())
    data = collect(*bundle)
    column = data['nodes']['model.flux_gold.monthly_song_metrics']['columns'][0]
    assert column['meaning_origin'] == 'shared column glossary'
    assert column['description'] == '반복 청취 횟수'


def test_changed_macro_logic_cannot_reuse_successful_results(bundle):
    root = bundle[0]
    mutate(root / 'target-docs/manifest.json', lambda d: d['macros'].update({
        'macro.flux_gold.count': {'macro_sql': 'select wrong_count'}}))
    with pytest.raises(ValueError, match='definition mismatch'):
        collect(*bundle)


def test_other_policy_cannot_use_fixed_utc_and_thirty_second_catalog_semantics(bundle):
    root = bundle[0]
    mutate(root / 'inputs.json', lambda d: d['context'].update(timezone='Asia/Seoul'))
    mutate(root / 'result.json', lambda d: d.update(input_manifest_sha256=sha256(root / 'inputs.json')))
    with pytest.raises(ValueError, match='input policy mismatch'):
        collect(*bundle)


def test_static_site_is_self_contained_escaped_and_does_not_modify_sources(bundle, tmp_path):
    root = bundle[0]
    for tag in ['docs', 'build', 'rebuild']:
        mutate(root / f'target-{tag}/manifest.json', lambda d: d['nodes'][
            'model.flux_gold.monthly_song_metrics'].update(description='</script><img src=x onerror=alert(1)>'))
    before = {p: sha256(p) for directory in bundle[:3] for p in directory.rglob('*') if p.is_file()}
    before[bundle[3]] = sha256(bundle[3])
    output = tmp_path / 'site'
    result = build(*bundle, output)
    assert result['completed'] and result['sources_unchanged']
    assert all(sha256(p) == digest for p, digest in before.items())
    html = (output / 'index.html').read_text(encoding='utf-8')
    assert '</script><img' not in html and '\\u003c/script' in html
    assert '.textContent' in html and 'innerHTML' not in html
    assert (output / 'dbt/index.html').exists()
    assert not list(output.rglob('*.parquet')) and not list(output.rglob('*.sqlite3'))
    assert sha256(output / 'catalog.json') == result['artifacts']['catalog.json']
    with pytest.raises((ValueError, FileExistsError)):
        build(*bundle, root)
    with pytest.raises((ValueError, FileExistsError)):
        build(*bundle, output)


@pytest.mark.skipif(shutil.which('node') is None, reason='offline JavaScript test requires Node')
def test_catalog_javascript_renders_searches_and_selects_without_browser(bundle, tmp_path):
    output = tmp_path / 'site'
    build(*bundle, output)
    result = subprocess.run(['node', str(Path(__file__).with_name('catalog_dom.cjs')), str(output / 'index.html')],
                            capture_output=True, text=True, timeout=15)
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)['passed']
