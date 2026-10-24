import ast
import json
import shutil
import time
from collections import Counter
from importlib.metadata import version
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from urllib.parse import unquote

from flux.common.execution import recorded, record_inputs, progress
from flux.validation.dbt_full import separate_output
from flux.validation.dbt_pilot import save
from flux.validation.pinning import pin_delta, sha256


TABLES = {'bronze':'bronze', 'monthly_user_recordings':'silver/monthly_user_recordings',
          'recording_metadata':'silver/recording_metadata', 'invalid_listens':'silver/invalid_listens'}
RULES = ['src/flux/silver/'+name for name in ['quality.py','build.py','dedup.py','mbid.py','schema.py',
         'monthly_listens.py','monthly_state.py','user_recording_counts.py','recording_link_policy.py',
         'name_matching_rules.json']]+['src/flux/validation/quality_rules.json','src/flux/review/journal.py']
PIN_FIELDS = ('id','version','log_sha256','schema','properties','monthly_state')
MAX_BYTES = 100*1024*1024
MAX_ROWS = 10000


def relative_path(root, name):
    if not isinstance(name,str) or not name or '\\' in name or ':' in name:
        raise ValueError('unsafe snapshot path')
    parts=PurePosixPath(name)
    if parts.is_absolute() or '..' in parts.parts or str(parts)!=name:
        raise ValueError('unsafe snapshot path')
    root=Path(root).resolve();path=root.joinpath(*parts.parts)
    if path.is_symlink() or root not in path.resolve().parents:
        raise ValueError('external snapshot reference')
    return path


def inventory(root):
    root=Path(root).resolve();result={}
    for path in sorted(root.rglob('*')):
        if path.is_symlink():raise ValueError('symlink not supported in snapshot')
        if path.is_file():
            if root not in path.resolve().parents:raise ValueError('external snapshot file')
            size=path.stat().st_size
            if size>MAX_BYTES:raise ValueError('bounded snapshot file required')
            result[path.relative_to(root).as_posix()]={'bytes':size,'sha256':sha256(path)}
    return result


def delta_snapshot(root):
    root=Path(root);pin=pin_delta(root);cdf_start=None;cdf_enabled=False
    for number in range(pin['version']+1):
        file=root/'_delta_log'/f'{number:020d}.json'
        for line in file.read_text(encoding='utf-8').splitlines():
            if not line.strip():continue
            action=json.loads(line)
            if 'metaData' in action:
                enabled=action['metaData'].get('configuration',{}).get('delta.enableChangeDataFeed')=='true'
                if enabled and not cdf_enabled:cdf_start=number
                if not enabled:cdf_start=None
                cdf_enabled=enabled
            for kind in ['add','remove','cdc']:
                if kind not in action:continue
                item=action[kind]
                path=relative_path(root,unquote(item['path']))
                if kind in ['add','cdc']:
                    if not path.is_file():raise ValueError('complete Delta data and CDF history required')
                    if path.stat().st_size!=item['size']:raise ValueError('historical Delta file size changed')
    return pin,{**{key:pin[key] for key in PIN_FIELDS},'cdf_start':cdf_start}


def canonical_rows(rows,schema):
    result=[]
    for row in rows:
        values=[]
        for value,field in zip(row,schema['fields']):
            if field['type']=='timestamp' and isinstance(value,datetime):
                value=(value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)).isoformat()
            values.append(value)
        result.append(values)
    result=json.loads(json.dumps(result,ensure_ascii=False,allow_nan=False,
                                 default=lambda value:value.isoformat() if hasattr(value,'isoformat') else str(value)))
    result.sort(key=lambda row:json.dumps(row,ensure_ascii=False,sort_keys=True))
    return result


def collected(pin):
    import duckdb

    fields=[field['name'] for field in pin['schema']['fields']]
    if not pin['files']:return {'columns':fields,'rows':[]}
    with duckdb.connect() as connection:
        connection.execute('set threads=2')
        connection.execute("set TimeZone='UTC'")
        connection.read_parquet([item['path'] for item in pin['files']],hive_partitioning=True,union_by_name=True).create_view('snapshot')
        names=','.join('"'+name.replace('"','""')+'"' for name in fields)
        rows=connection.execute('select '+names+' from snapshot limit '+str(MAX_ROWS+1)).fetchall()
    if len(rows)>MAX_ROWS:raise ValueError('bounded snapshot rows required')
    return {'columns':fields,'rows':canonical_rows(rows,pin['schema'])}


def monthly_values(table):
    names=table['columns'];keys=['user_name','recording_msid','listened_month']
    positions=[names.index(key) for key in keys];count=names.index('play_count');result={}
    for row in table['rows']:
        key=tuple(row[index] for index in positions);value=row[count]
        if (any(not isinstance(item,str) or not item.strip() for item in key)
                or key in result or type(value) is not int or value<1):raise ValueError('invalid monthly key or play_count')
        result[key]=value
    return result


def policy_minimum(root):
    tree=ast.parse((Path(root)/'rules/src/flux/silver/quality.py').read_text(encoding='utf-8'))
    values=[ast.literal_eval(node.value) for node in tree.body if isinstance(node,ast.Assign)
            and any(isinstance(target,ast.Name) and target.id=='MIN_MS_PLAYED' for target in node.targets)]
    if values!=[30000]:raise ValueError('30 second quality policy required')
    return values[0]


def cursor_for(snapshots):
    state=snapshots['monthly_user_recordings']['monthly_state']
    expected=dict(source_id=snapshots['bronze']['id'],version=snapshots['bronze']['version'],
                  key=['user_name','recording_msid','listened_month'],minimum_ms_played=30000,policy_version=1,timezone='UTC')
    if state!=expected or any(snapshots[name]['monthly_state']!=state for name in TABLES if name!='bronze'):
        raise ValueError('inconsistent source checkpoint or policy')
    return {'kind':'monthly-silver-input-checkpoint','state':state,'paths':TABLES,
            'tables':{name:{key:table[key] for key in ['id','version','log_sha256']} for name,table in snapshots.items()}}


def approvals_summary(path):
    data=json.loads(Path(path).read_text(encoding='utf-8'))
    count=data['approved_candidates'];human=data['human_approved_candidates'];mappings=data['mappings']
    if (data.get('mode')!='production' or type(count) is not int or count<0 or human!=count
            or not isinstance(mappings,list) or bool(count)!=bool(mappings)
            or any(row.get('reviewer_kind')!='human' for row in mappings)):
        raise ValueError('consistent production mapping export required')
    return {'approved_candidates':count,'human_approved_candidates':human,'mapping_rows':len(mappings)}


def write_json(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')


def sealed_manifest(root,kind):
    root=Path(root).resolve();result=json.loads((root/'result.json').read_text(encoding='utf-8'))
    if result.get('completed') is not True or result.get('kind')!=kind:raise ValueError('complete snapshot required')
    if sha256(root/'manifest.json')!=result['manifest_sha256']:raise ValueError('snapshot manifest changed')
    manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
    if manifest.get('format_version')!=1 or manifest.get('table_paths')!=TABLES:raise ValueError('unsupported snapshot layout')
    for name in manifest['files']:relative_path(root,name)
    found=inventory(root)
    for name in ['result.json','manifest.json']:found.pop(name,None)
    if found!=manifest['files']:raise ValueError('snapshot payload missing, changed or unexpected')
    return manifest


def compare_snapshot(root,manifest):
    root=Path(root);snapshots={};actual={}
    for name,relative in TABLES.items():
        pin,snapshot=delta_snapshot(root/relative)
        if snapshot!=manifest['tables'][name]:raise ValueError('restored Delta identity or history changed')
        snapshots[name]=snapshot;actual[name]=collected(pin)
    cursor=json.loads((root/'context/cursor.json').read_text(encoding='utf-8'))
    if cursor!=cursor_for(snapshots):raise ValueError('restored checkpoint changed')
    policy_minimum(root)
    if approvals_summary(root/'context/approved_mappings.json')!=manifest['approvals']:raise ValueError('approved mapping summary changed')
    expected=json.loads((root/'context/expected.json').read_text(encoding='utf-8'))
    if set(expected)!=set(TABLES):raise ValueError('missing expected table')
    wanted=monthly_values(expected['monthly_user_recordings']);found=monthly_values(actual['monthly_user_recordings'])
    if wanted!=found:raise ValueError('monthly key and play_count mismatch')
    if actual!=expected:raise ValueError('restored row or column mismatch')
    return {'tables_checked':len(TABLES),'monthly_rows':len(found),'plays':sum(found.values()),
            'key_mismatches':0,'play_count_mismatches':0,'all_columns_match':True,
            'table_rows':{name:len(table['rows']) for name,table in actual.items()}}


@recorded('delta-snapshot-backup')
def backup(source,output,project,approval):
    source,output,project,approval=map(lambda value:Path(value).resolve(),[source,output,project,approval])
    separate_output(output,[source,approval,*[project/name for name in RULES]])
    if (source/'silver/.writer.lock').exists():raise ValueError('active Silver writer')
    pins={};snapshots={};before={}
    for name,relative in TABLES.items():
        table=source/relative
        before[name]=inventory(table)
        if any(Path(path).name=='.writer.lock' for path in before[name]):raise ValueError('active table writer')
        pins[name],snapshots[name]=delta_snapshot(table)
    if sum(item['bytes'] for files in before.values() for item in files.values())>MAX_BYTES:raise ValueError('bounded backup bytes required')
    cursor=cursor_for(snapshots);approved=approvals_summary(approval)
    assets={name:sha256(project/name) for name in RULES};approval_hash=sha256(approval)
    expected={name:collected(pin) for name,pin in pins.items()}
    monthly_values(expected['monthly_user_recordings'])
    record_inputs({'delta_tables':cursor['tables'],'source_state':cursor['state'],'rules_sha256':assets,
                   'approved_mappings_sha256':approval_hash})
    output.mkdir(parents=True,exist_ok=False);start=time.perf_counter()
    report=dict(completed=False,kind='bounded-delta-backup-v1',production_applied=False,stage='copy pinned payload')
    save(output,report)
    try:
        for name,relative in TABLES.items():
            shutil.copytree(source/relative,output/relative)
            if inventory(output/relative)!=before[name]:raise ValueError('copied Delta files changed')
            progress({'table':name,'copied_files':len(before[name])})
        for relative in RULES:
            target=output/'rules'/relative;target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(project/relative,target)
            if sha256(target)!=assets[relative]:raise ValueError('copied rule changed')
        target=output/'context/approved_mappings.json';target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(approval,target)
        if sha256(target)!=approval_hash:raise ValueError('copied approvals changed')
        write_json(output/'context/cursor.json',cursor);write_json(output/'context/expected.json',expected)
        policy_minimum(output)
        for name,relative in TABLES.items():
            if inventory(source/relative)!=before[name]:raise ValueError('source advanced during backup')
        if (source/'silver/.writer.lock').exists() or any(sha256(project/name)!=digest for name,digest in assets.items()) or sha256(approval)!=approval_hash:
            raise ValueError('source rules, approvals or writer state changed')
        payload=inventory(output);payload.pop('result.json')
        manifest=dict(format_version=1,scope='complete retained history of bounded source; not production promotion',
                      table_paths=TABLES,tables=snapshots,approvals=approved,files=payload)
        write_json(output/'manifest.json',manifest)
        report.update(stage='compare copied snapshot');save(output,report)
        comparison=compare_snapshot(output,manifest)
        report.update(completed=True,source_unchanged=True,comparison=comparison,approvals=approved,
                      manifest_sha256=sha256(output/'manifest.json'),payload_bytes=sum(f['bytes'] for f in payload.values()),
                      payload_files=len(payload),elapsed_seconds=round(time.perf_counter()-start,3))
    except Exception as error:
        report['failure']=type(error).__name__+': '+str(error);save(output,report);raise
    save(output,report)
    return report


@recorded('delta-snapshot-restore')
def restore(saved,output):
    saved,output=Path(saved).resolve(),Path(output).resolve();separate_output(output,[saved])
    manifest=sealed_manifest(saved,'bounded-delta-backup-v1')
    saved_before=inventory(saved)
    compare_snapshot(saved,manifest)
    record_inputs({'delta_tables':{name:{key:table[key] for key in ['id','version','log_sha256']} for name,table in manifest['tables'].items()},
                   'backup_manifest_sha256':sha256(saved/'manifest.json')})
    output.mkdir(parents=True,exist_ok=False);start=time.perf_counter()
    report=dict(completed=False,kind='restored-delta-snapshot-v1',production_applied=False,source_paths_used=False,stage='restore local payload')
    save(output,report)
    try:
        for name in manifest['files']:
            target=relative_path(output,name);target.parent.mkdir(parents=True,exist_ok=True)
            shutil.copyfile(relative_path(saved,name),target)
        shutil.copyfile(saved/'manifest.json',output/'manifest.json')
        if inventory(saved)!=saved_before:raise ValueError('backup changed during restore')
        sealed_manifest(saved,'bounded-delta-backup-v1')
        report['stage']='compare restored snapshot';save(output,report)
        comparison=compare_snapshot(output,manifest)
        files=inventory(output);files.pop('result.json');files.pop('manifest.json')
        if files!=manifest['files']:raise ValueError('restored payload mismatch')
        report.update(completed=True,comparison=comparison,manifest_sha256=sha256(output/'manifest.json'),
                      elapsed_seconds=round(time.perf_counter()-start,3),payload_files=len(files),
                      payload_bytes=sum(f['bytes'] for f in files.values()))
    except Exception as error:
        report['failure']=type(error).__name__+': '+str(error);save(output,report);raise
    save(output,report)
    return report


def verify_restored(root):
    manifest=sealed_manifest(root,'restored-delta-snapshot-v1')
    return compare_snapshot(root,manifest)


@recorded('delta-independent-restore-verification')
def verify_delta(restored,output):
    from deltalake import DeltaTable
    import pyarrow as pa

    restored,output=Path(restored).resolve(),Path(output).resolve()
    separate_output(output,[restored]);manifest=sealed_manifest(restored,'restored-delta-snapshot-v1')
    original=inventory(restored);expected=json.loads((restored/'context/expected.json').read_text(encoding='utf-8'))
    record_inputs({'delta_tables':{name:{key:table[key] for key in ['id','version','log_sha256']} for name,table in manifest['tables'].items()}})
    output.mkdir(parents=True,exist_ok=False);started=time.perf_counter()
    report=dict(completed=False,production_applied=False,source_paths_used=False,stage='read restored Delta',tables={})
    save(output,report)
    try:
        for name,relative in TABLES.items():
            path=restored/relative;pin=manifest['tables'][name]
            table=DeltaTable(str(path),version=pin['version'])
            rows=table.to_pyarrow_table(columns=expected[name]['columns']).to_pylist()
            if len(rows)>MAX_ROWS:raise ValueError('bounded Delta snapshot required')
            actual=canonical_rows([[row[key] for key in expected[name]['columns']] for row in rows],pin['schema'])
            if actual!=expected[name]['rows']:raise ValueError('Delta reader restored key or value mismatch: '+name)
            versions=[row['version'] for row in table.history()]
            if table.metadata().id!=pin['id'] or set(versions)!=set(range(pin['version']+1)):raise ValueError('restored Delta history mismatch')
            for file in table.file_uris():
                if path.resolve() not in Path(file).resolve().parents:raise ValueError('Delta reader used an external data path')
            first=DeltaTable(str(path),version=0).to_pyarrow_table().num_rows
            cdf={}
            if pin['cdf_start'] is not None:
                feed=pa.table(table.load_cdf(starting_version=pin['cdf_start'],ending_version=pin['version']).read_all())
                if feed.num_rows>100000:raise ValueError('bounded CDF history required')
                cdf=dict(Counter(feed['_change_type'].to_pylist()))
            report['tables'][name]=dict(rows=len(rows),version=pin['version'],history_versions=len(versions),
                                        version_zero_rows=first,cdf_actions=cdf,active_paths_local=True)
            save(output,report);progress({'table':name,'rows_checked':len(rows),'cdf_rows':sum(cdf.values())})
        report['stage']='verify restored files unchanged';save(output,report)
        report['comparison']=verify_restored(restored)
        if inventory(restored)!=original:raise ValueError('restored source changed during read')
        report.update(completed=True,restored_unchanged=True,reader='delta-rs',reader_version=version('deltalake'),
                      elapsed_seconds=round(time.perf_counter()-started,3),approvals=manifest['approvals'])
    except Exception as error:
        report['failure']=type(error).__name__+': '+str(error);save(output,report);raise
    save(output,report)
    return report
