import json
import shutil
import sqlite3
import time
from collections import Counter
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from flux.common.execution import recorded, record_inputs
from flux.validation.oracle import KEYS
from flux.validation.pinning import sha256
from flux.validation.quality_contract import registry


ROOT = Path(__file__).parent
SOURCE_GRAINS = {'monthly': ['user_name', 'recording_msid', 'listened_month'],
                 'song_dimension': ['recording_msid'], 'song_labels': ['song_key'],
                 'song_credits': ['song_key', 'artist_mbid']}
COMMON_COLUMNS = {'period_start': 'UTC 청취 기간의 첫날. 도착 시각이 아니다.',
                  'user_name': '청취 계정 이름. 실제 사람 수로 해석하지 않는다.',
                  'plays': '해당 사용자와 기간의 반복 청취 횟수 합.',
                  'status': '고정 곡 식별 정책의 연결 상태.',
                  'recording_msid': '수집된 녹음 메타데이터 식별자. 곡명 자체가 아니다.',
                  'recording_mbid': 'MusicBrainz 녹음 ID. 결측을 허용한다.'}


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def contained(root, name):
    path = (root / name).resolve()
    if root not in path.parents:
        raise ValueError('evidence path escapes root')
    return path


def grain(name):
    if name in SOURCE_GRAINS:
        return SOURCE_GRAINS[name]
    if name.startswith('stg_'):
        return SOURCE_GRAINS.get(name[4:], [])
    if name.startswith('int_user_'):
        suffix = name.rsplit('_', 1)[1]
        return ['period_start', 'user_name', {'songs': 'song_key', 'artists': 'artist_mbid',
                                            'variants': 'variant'}[suffix]]
    return KEYS.get(name.split('_', 1)[-1], [])


def signature(manifest):
    fields = ['resource_type', 'name', 'description', 'columns', 'checksum', 'config',
              'raw_code', 'meta', 'identifier', 'schema', 'database']
    return {group: {key: {**{field: node.get(field) for field in fields},
                         'parents': node.get('depends_on', {}).get('nodes', [])}
                    for key, node in manifest.get(group, {}).items()}
            for group in ['nodes', 'sources']} | {
                'macros': {key: node.get('macro_sql') for key, node in manifest.get('macros', {}).items()}}


def execution(path, root, info, report):
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)) as connection:
        rows = connection.execute('select kind,at,elapsed_seconds,data,stage,stage_seconds '
                                  'from events order by seq').fetchall()
    if not rows or rows[0][0] != 'run_started' or rows[-1][0] != 'run_finished':
        raise ValueError('execution is unfinished; no terminal evidence')
    start, end = json.loads(rows[0][3]), json.loads(rows[-1][3])
    inputs, metrics = {}, {}
    for kind, _, _, payload, _, _ in rows:
        event = json.loads(payload)
        if kind == 'inputs':
            inputs.update(event)
        elif kind == 'checkpoint':
            inputs.update(event['inputs'])
            metrics.update(event['metrics'])
    expected = {'baseline_sha256': info['baseline_sha256'],
                'input_manifest_sha256': report['input_manifest_sha256'],
                'project_sha256': report['project_sha256'], 'context': info['context']}
    if (start['job'] != 'dbt-sample' or Path(start['parameters']['root']).resolve() != root
            or any(inputs.get(k) != v for k, v in expected.items())
            or metrics.get('final_comparison') != report['final_comparison']):
        raise ValueError('execution input/output mismatch')
    return {'run_id': start['run_id'], 'job': start['job'], 'status': end['status'],
            'started_at': rows[0][1], 'finished_at': rows[-1][1], 'seconds': rows[-1][2],
            'event_count': len(rows), 'error_recorded': end.get('error') is not None,
            'input_match': True,
            'stages': [{'name': row[4], 'at': row[1], 'seconds': row[5],
                        'status': json.loads(row[3])['status']}
                       for row in rows if row[0] == 'stage_finished']}


def external_lineage(context, nodes):
    descriptions = {
        'bronze': 'ListenBrainz 원본 청취. 원본을 보존한다.',
        'quality': '필수값 검사와 알려진 30초 미만 재생 격리. 재생 시간 결측은 유지한다.',
        'dedup': '동일 사용자와 MSID 및 청취 시각의 동일 청취 중복 제거.',
        'monthly': '사용자와 MSID 및 UTC 청취 월로 묶고 반복 청취를 play_count로 보존한다.',
        'recording_metadata': 'Silver에 남긴 원본 MSID의 메타데이터.',
        'identity_policy': '고정한 곡 연결 정책. Live, Remix와 Sped Up을 구분한다.',
        'credit_policy': '공식 메인과 피처링 아티스트 귀속. 새 사람 승인은 추가하지 않는다.'}
    pins = {'bronze': 'bronze', 'monthly': 'monthly_user_recordings', 'recording_metadata': 'recording_metadata'}
    external = {key: {'name': key, 'description': description,
                     'pin': {field: context.get('tables', {}).get(pins.get(key), {}).get(field)
                             for field in ['id', 'version']} if key in pins else None}
                for key, description in descriptions.items()}
    pairs = [('bronze', 'quality'), ('quality', 'dedup'), ('dedup', 'monthly'),
             ('dedup', 'recording_metadata'), ('recording_metadata', 'identity_policy'),
             ('identity_policy', 'credit_policy')]
    for key, node in nodes.items():
        if node['kind'] == 'source':
            pairs.append(({'monthly': 'monthly', 'song_dimension': 'identity_policy',
                           'song_labels': 'identity_policy', 'song_credits': 'credit_policy'}[node['name']], key))
    return {'scope': 'documented external lineage, not automatic Spark runtime capture', 'nodes': external,
            'edges': [{'parent': parent, 'child': child, 'kind': 'declared'} for parent, child in pairs]}


def collect(dbt_root, quality_root, gx_root, journal):
    root, quality_root, gx_root, journal = map(lambda p: Path(p).resolve(),
                                             [dbt_root, quality_root, gx_root, journal])
    evidence = {}

    def load(path, label):
        digest = sha256(path)
        value = read(path)
        if sha256(path) != digest:
            raise ValueError('evidence changed while reading')
        evidence[label] = {'sha256': digest, 'file': path.name}
        return value

    info = load(root / 'inputs.json', 'dbt_inputs')
    report = load(root / 'result.json', 'dbt_result')
    if (report.get('completed') is not True or report.get('production_applied') is not False
            or info.get('production_applied') is not False
            or report['input_manifest_sha256'] != evidence['dbt_inputs']['sha256']):
        raise ValueError('dbt execution is incomplete or input manifest changed')
    required_context = {'listener_key': 'user_name', 'timezone': 'UTC', 'minimum_ms_played': 30000,
                        'r9_reference_only': True, 'approved_mappings': 0}
    if any(info['context'].get(k) != value for k, value in required_context.items()):
        raise ValueError('catalog input policy mismatch')
    baseline = Path(info['baseline_path']).resolve()
    baseline_report = load(baseline / 'result.json', 'baseline')
    if (not baseline_report.get('completed') or info['baseline_sha256'] != evidence['baseline']['sha256']):
        raise ValueError('baseline mismatch')
    source_context = load(baseline / 'source_context.json', 'source_context')
    expected_context = baseline_report.get('artifacts', {}).get('source_context.json')
    if expected_context and evidence['source_context']['sha256'] != expected_context:
        raise ValueError('pinned source context changed')
    for name, digest in report['project_sha256'].items():
        path = contained(root / 'project', name)
        if sha256(path) != digest:
            raise ValueError('dbt project changed')
        evidence['project/' + name] = {'sha256': digest, 'file': path.name}
    contract = load(quality_root / 'result.json', 'quality_contract')
    gx = load(gx_root / 'result.json', 'gx_result')
    rules = registry()
    registry_path = ROOT / 'quality_rules.json'
    evidence['rule_registry'] = {'sha256': sha256(registry_path), 'file': registry_path.name}
    for item in [contract, gx]:
        if (item.get('baseline_manifest_sha256') != info['baseline_sha256']
                or item.get('production_applied') is not False):
            raise ValueError('quality baseline/scope mismatch')
    for item in [contract, gx['contract']]:
        if (not item.get('evaluation_completed') or item.get('policy_version') != rules['version']
                or (item is contract and item.get('registry_sha256') != sha256(registry_path))):
            raise ValueError('quality registry mismatch or incomplete evaluation')
        values = item['profile']['values']
        comparison = report['final_comparison']
        if (values['monthly_rows'], values['plays']) != (comparison['source_rows'], comparison['plays']):
            raise ValueError('quality population mismatch')
        if item is not contract and item['profile'].get('scope_id') != info['baseline_sha256']:
            raise ValueError('GX embedded contract scope mismatch')
    if not gx.get('completed') or not gx['gx'].get('completed') or gx['monthly_rows'] != comparison['source_rows']:
        raise ValueError('GX incomplete or population mismatch')
    docs = root / 'target-docs'
    manifest = load(docs / 'manifest.json', 'dbt_manifest')
    catalog = load(docs / 'catalog.json', 'dbt_catalog')
    if manifest['metadata']['invocation_id'] != catalog['metadata']['invocation_id']:
        raise ValueError('dbt documentation invocation mismatch')
    evidence['dbt_index'] = {'sha256': sha256(docs / 'index.html'), 'file': 'index.html'}
    resources = {**manifest['sources'], **{k: v for k, v in manifest['nodes'].items()
                                          if v['resource_type'] == 'model'}}
    if any(s.get('meta', {}).get('baseline_sha256') != info['baseline_sha256']
           for s in manifest['sources'].values()):
        raise ValueError('dbt source baseline mismatch')
    tests = {k: v for k, v in manifest['nodes'].items() if v['resource_type'] == 'test'}
    runs = {}
    for tag in ['build', 'rebuild']:
        other = load(root / f'target-{tag}/manifest.json', tag + '_manifest')
        results = load(root / f'target-{tag}/run_results.json', tag + '_results')
        if signature(manifest) != signature(other):
            raise ValueError('model definition mismatch between docs and execution')
        if other['metadata']['invocation_id'] != results['metadata']['invocation_id']:
            raise ValueError('dbt results invocation mismatch')
        expected = set(tests) | {k for k, v in resources.items() if v['resource_type'] == 'model'}
        found = [r['unique_id'] for r in results['results']]
        if set(found) != expected or len(found) != len(expected):
            raise ValueError('missing or duplicate dbt execution result')
        runs[tag] = {r['unique_id']: {'status': r['status'], 'seconds': r['execution_time'],
                                    'timing': r.get('timing', []),
                                    'invocation_id': results['metadata']['invocation_id']}
                     for r in results['results']}
    checks = {r['rule_id']: r for r in contract['checks']}
    rule_ids = {r['rule_id'] for r in rules['rules']}
    if (set(checks) != rule_ids or len(checks) != len(contract['checks'])
            or any(c['status'] not in {'PASS', 'FAIL', 'WARN', 'SKIP'} for c in checks.values())):
        raise ValueError('quality rule result set mismatch')
    gx_checks = {r['rule_id']: {k: r[k] for k in ['success', 'expectations']} for r in gx['gx']['checks']}
    if set(gx_checks) != {'DQ003', 'DQ004', 'DQ005'} or len(gx_checks) != len(gx['gx']['checks']):
        raise ValueError('GX rule result set mismatch')
    gx_contract_checks = {r['rule_id']: r for r in gx['contract']['checks']}
    if (set(gx_contract_checks) != rule_ids or len(gx_contract_checks) != len(gx['contract']['checks'])
            or any(gx_contract_checks[r['rule_id']]['severity'] != r['severity']
                   or checks[r['rule_id']]['severity'] != r['severity'] for r in rules['rules'])):
        raise ValueError('quality rule ownership mismatch')
    exported_rules = {r['rule_id']: {**r, 'contract_status': checks[r['rule_id']]['status'],
                                    'gx_contract_status': gx_contract_checks[r['rule_id']]['status'],
                                    'gx': gx_checks.get(r['rule_id'])} for r in rules['rules']}
    descriptions = {**{name: column['description']
                                        for node in resources.values() for name, column in node['columns'].items()
                                        if column.get('description')}, **COMMON_COLUMNS}
    nodes = {}
    for key, node in resources.items():
        physical = catalog['sources' if node['resource_type'] == 'source' else 'nodes'].get(key)
        if physical is None:
            raise ValueError('catalog is missing a relation')
        columns = physical['columns']
        if not set(node['columns']) <= set(columns):
            raise ValueError('catalog column mismatch')
        parents = node.get('depends_on', {}).get('nodes', [])
        if any(parent not in resources for parent in parents):
            raise ValueError('unknown lineage parent')
        direct = node.get('config', {}).get('meta', {}).get('rule_ids', [])
        if not set(direct) <= rule_ids:
            raise ValueError('unknown model quality rule')
        nodes[key] = {'id': key, 'name': node['name'], 'kind': node['resource_type'],
                      'description': node.get('description', ''), 'grain': grain(node['name']),
                      'owner': 'flux.gold' if node['resource_type'] == 'model' else 'flux.silver',
                      'parents': parents, 'children': [], 'rule_ids': direct, 'dbt_checks': [],
                      'runs': {tag: values[key] for tag, values in runs.items() if key in values},
                      'columns': [{'name': name, 'type': column['type'],
                                   'description': node['columns'].get(name, {}).get('description')
                                   or descriptions.get(name, '문서 미작성'),
                                   'meaning_origin': 'dbt' if node['columns'].get(name, {}).get('description')
                                   else 'shared column glossary' if name in descriptions else 'missing'}
                                  for name, column in columns.items()]}
    visiting, visited = set(), set()

    def ancestors(key):
        if key in visiting:
            raise ValueError('cyclic lineage')
        if key not in visited:
            visiting.add(key)
            inherited = set()
            for parent in nodes[key]['parents']:
                ancestors(parent)
                nodes[parent]['children'].append(key)
                inherited.update(nodes[parent]['rule_ids'])
                inherited.update(nodes[parent]['upstream_rule_ids'])
            nodes[key]['upstream_rule_ids'] = sorted(inherited - set(nodes[key]['rule_ids']))
            visited.add(key)
            visiting.remove(key)

    for key in nodes:
        ancestors(key)
    for key, test in tests.items():
        for parent in test['depends_on']['nodes']:
            if parent not in nodes:
                raise ValueError('test lineage references unknown model')
            nodes[parent]['dbt_checks'].append({'id': key, 'name': test['name'],
                                               'runs': {tag: values[key] for tag, values in runs.items()}})
    journal_digest = sha256(journal)
    run = execution(journal, root, info, report)
    if sha256(journal) != journal_digest:
        raise ValueError('execution journal changed during reading')
    evidence['execution_journal'] = {'sha256': journal_digest, 'file': journal.name}
    return {'schema_version': 1, 'generated_at': datetime.now(timezone.utc).isoformat(),
            'scope': 'fixed sample only; not production monitoring',
            'production_applied': False, 'promotion_ready': False,
            'baseline_sha256': info['baseline_sha256'], 'context': info['context'],
            'population': {k: comparison[k] for k in ['source_rows', 'plays', 'tables_checked', 'mismatches']},
            'nodes': nodes, 'rules': exported_rules, 'execution': run, 'evidence': evidence,
            'external_lineage': external_lineage(source_context, nodes),
            'quality': {'summary': dict(Counter(c['status'] for c in checks.values())),
                        'scope_rule_ids': [r['rule_id'] for r in rules['rules']
                                           if r['stage'] in ['input', 'review', 'collection', 'observability']],
                        'gx_passed': gx['gx']['passed'], 'contract_passed': contract['quality_passed'],
                        'gx_registry_binding': 'policy version and rule IDs; legacy report has no registry digest'},
            'counts': {'models': sum(n['kind'] == 'model' for n in nodes.values()),
                       'sources': len(manifest['sources']), 'tests': len(tests),
                       'undocumented_columns': sum(c['meaning_origin'] == 'missing'
                                                   for n in nodes.values() for c in n['columns'])}}


@recorded('data-catalog')
def build(dbt_root, quality_root, gx_root, journal, output):
    begin = time.perf_counter()
    roots = [Path(p).resolve() for p in [dbt_root, quality_root, gx_root]]
    output = Path(output).resolve()
    baseline = Path(read(roots[0] / 'inputs.json')['baseline_path']).resolve()
    for source in [*roots, baseline, Path(journal).resolve()]:
        if output == source or output in source.parents or source in output.parents:
            raise ValueError('catalog output overlaps evidence')
    if output.exists():
        raise FileExistsError(output)
    data = collect(*roots, journal)
    record_inputs({'baseline_sha256': data['baseline_sha256'], 'evidence': data['evidence']})
    output.mkdir(parents=True, exist_ok=False)
    payload = json.dumps(data, ensure_ascii=False, indent=2, allow_nan=False)
    (output / 'catalog.json').write_text(payload + '\n', encoding='utf-8')
    escaped = payload.replace('&', '\\u0026').replace('<', '\\u003c').replace('>', '\\u003e')
    escaped = escaped.replace('\u2028', '\\u2028').replace('\u2029', '\\u2029')
    template = (ROOT / 'catalog.html').read_text(encoding='utf-8')
    (output / 'index.html').write_text(template.replace('__CATALOG_JSON__', escaped), encoding='utf-8')
    (output / 'dbt').mkdir()
    for name, label in [('index.html', 'dbt_index'), ('manifest.json', 'dbt_manifest'), ('catalog.json', 'dbt_catalog')]:
        shutil.copyfile(roots[0] / 'target-docs' / name, output / 'dbt' / name)
        if sha256(output / 'dbt' / name) != data['evidence'][label]['sha256']:
            raise ValueError('dbt docs changed during copy')
    after = collect(*roots, journal)
    if after['evidence'] != data['evidence']:
        raise ValueError('catalog evidence changed during generation')
    result = {'completed': True, 'production_applied': False, 'promotion_ready': False,
              'sources_unchanged': True, 'scope': data['scope'], 'counts': data['counts'],
              'quality': data['quality']['summary'], 'baseline_sha256': data['baseline_sha256'],
              'source_execution_id': data['execution']['run_id'],
              'seconds': round(time.perf_counter() - begin, 3),
              'artifacts': {p.relative_to(output).as_posix(): sha256(p)
                            for p in sorted(output.rglob('*')) if p.is_file()},
              'payload_bytes': sum(p.stat().st_size for p in output.rglob('*') if p.is_file())}
    temp = output / 'result.tmp'
    temp.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    temp.replace(output / 'result.json')
    return result
