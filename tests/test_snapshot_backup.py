import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

import duckdb
import pytest

from flux.validation.snapshot_backup import backup, restore, verify_restored
from flux.validation.pinning import sha256


PROJECT = Path(__file__).resolve().parents[1]
TABLES = {'bronze': 'bronze', 'monthly_user_recordings': 'silver/monthly_user_recordings',
          'recording_metadata': 'silver/recording_metadata', 'invalid_listens': 'silver/invalid_listens'}


@pytest.fixture
def sample(tmp_path, monkeypatch):
    monkeypatch.setenv('FLUX_EXECUTION_ROOT', str(tmp_path/'executions'))
    source = tmp_path/'source'
    state = dict(source_id='bronze-id', version=1, key=['user_name','recording_msid','listened_month'],
                 minimum_ms_played=30000, policy_version=1, timezone='UTC')
    for name, relative in TABLES.items():
        root = source/relative
        (root/'_delta_log').mkdir(parents=True)
        schema = [('value','string')]
        if name == 'monthly_user_recordings':
            schema = [('user_name','string'),('recording_msid','string'),('listened_month','string'),('play_count','long')]
        elif name == 'recording_metadata':
            schema = [('recording_msid','string'),('track_name','string')]
        for version in [0,1]:
            file = root/f'part-{version}.parquet'
            with duckdb.connect() as connection:
                if name == 'monthly_user_recordings':
                    connection.execute("create table t as select 'u' as user_name, 's' as recording_msid, '2026-01' as listened_month, ?::bigint as play_count",[version+1])
                elif name == 'recording_metadata':
                    connection.execute("create table t as select 's' as recording_msid, 'Song' as track_name")
                else:
                    connection.execute("create table t as select 'v' as value" + (' where false' if name == 'invalid_listens' else ''))
                connection.execute('copy t to ? (format parquet)',[str(file)])
            actions = [{'commitInfo': {'userMetadata': json.dumps({'monthly_silver':state})}}]
            if version == 0:
                actions += [{'protocol': {'minReaderVersion':1,'minWriterVersion':4}}, {'metaData': {
                    'id':'bronze-id' if name=='bronze' else name+'-id', 'format':{'provider':'parquet','options':{}},
                    'schemaString':json.dumps({'type':'struct','fields':[{'name':n,'type':t,'nullable':True,'metadata':{}} for n,t in schema]}),
                    'partitionColumns':[],'configuration':{'delta.enableChangeDataFeed':'true'}}}]
            else:
                actions += [{'remove':{'path':'part-0.parquet','deletionTimestamp':1,'dataChange':True}}]
            actions += [{'add':{'path':file.name,'size':file.stat().st_size,'partitionValues':{},'modificationTime':1,'dataChange':True}}]
            (root/'_delta_log'/f'{version:020d}.json').write_text('\n'.join(json.dumps(a) for a in actions)+'\n',encoding='utf-8')
    approval = tmp_path/'approvals.json'
    approval.write_text(json.dumps(dict(mode='production',approved_candidates=0,human_approved_candidates=0,mappings=[],excluded=[])),encoding='utf-8')
    return source, approval


def make_backup(sample, tmp_path):
    source, approval = sample
    output = tmp_path/'backup'
    result = backup(source, output, PROJECT, approval)
    assert result['completed']
    return output


def test_restore_needs_only_backup_and_preserves_all_keys_values_history_and_assets(sample,tmp_path):
    source, _ = sample
    saved = make_backup(sample,tmp_path)
    source.rename(tmp_path/'unavailable-source')
    result = restore(saved,tmp_path/'restored')
    assert result['completed'] and result['comparison']['monthly_rows']==1
    assert result['comparison']['plays']==2
    assert result['comparison']['tables_checked']==4
    assert result['comparison']['all_columns_match']
    assert result['source_paths_used'] is False
    target=tmp_path/'restored'
    assert (target/'silver/monthly_user_recordings/part-0.parquet').exists()
    assert (target/'silver/monthly_user_recordings/_delta_log/00000000000000000000.json').exists()
    assert (target/'context/approved_mappings.json').read_bytes()==(saved/'context/approved_mappings.json').read_bytes()
    assert (target/'rules/src/flux/silver/quality.py').exists()
    assert verify_restored(target)['plays']==2


@pytest.mark.parametrize('damage',['data','log','retired','approval','rule','cursor','expected','extra','incomplete'])
def test_missing_or_changed_backup_is_rejected_before_creating_restore_target(sample,tmp_path,damage):
    saved=make_backup(sample,tmp_path)
    paths={'data':'silver/monthly_user_recordings/part-1.parquet',
           'log':'silver/monthly_user_recordings/_delta_log/00000000000000000000.json',
           'retired':'silver/monthly_user_recordings/part-0.parquet','approval':'context/approved_mappings.json',
           'rule':'rules/src/flux/silver/quality.py','cursor':'context/cursor.json','expected':'context/expected.json'}
    if damage=='extra':(saved/'unexpected').write_text('x')
    elif damage=='incomplete':
        report=json.loads((saved/'result.json').read_text());report['completed']=False
        (saved/'result.json').write_text(json.dumps(report))
    else:(saved/paths[damage]).unlink()
    with pytest.raises((ValueError,FileNotFoundError)):
        restore(saved,tmp_path/'restored')
    assert not (tmp_path/'restored').exists()


@pytest.mark.parametrize('damage',['writer','missing-history','missing-retired-data','missing-cdf','external-path','state-mismatch'])
def test_source_must_be_complete_local_and_consistent(sample,tmp_path,damage):
    source,approval=sample
    table=source/'silver/monthly_user_recordings'
    log=table/'_delta_log/00000000000000000000.json'
    if damage=='writer':(source/'silver/.writer.lock').write_text('123')
    elif damage=='missing-history':log.unlink()
    elif damage=='missing-retired-data':(table/'part-0.parquet').unlink()
    elif damage=='missing-cdf':
        with log.open('a') as stream:stream.write(json.dumps({'cdc':{'path':'_change_data/missing.parquet','size':1}})+'\n')
    else:
        content=log.read_text()
        if damage=='external-path':content=content.replace('part-0.parquet','../escape.parquet')
        else:content=content.replace('30000','10000')
        log.write_text(content)
        if damage=='state-mismatch':
            end=table/'_delta_log/00000000000000000001.json'
            end.write_text(end.read_text().replace('30000','10000'))
    with pytest.raises((ValueError,FileNotFoundError)):
        backup(source,tmp_path/'backup',PROJECT,approval)
    assert not (tmp_path/'backup/result.json').exists() or not json.loads((tmp_path/'backup/result.json').read_text())['completed']


def test_overlap_and_existing_restore_are_never_overwritten(sample,tmp_path):
    source,approval=sample
    with pytest.raises(ValueError):backup(source,source/'child',PROJECT,approval)
    saved=make_backup(sample,tmp_path)
    existing=tmp_path/'existing';existing.mkdir();(existing/'keep').write_text('keep')
    with pytest.raises(FileExistsError):restore(saved,existing)
    assert (existing/'keep').read_text()=='keep'
    with pytest.raises(ValueError):restore(saved,saved/'child')


def test_restored_data_is_compared_by_key_instead_of_totals(sample,tmp_path):
    saved=make_backup(sample,tmp_path)
    target=tmp_path/'restored';restore(saved,target)
    expected=target/'context/expected.json'
    data=json.loads(expected.read_text())
    data['monthly_user_recordings']['rows'][0][0]='wrong-user'
    expected.write_text(json.dumps(data))
    manifest_path=target/'manifest.json';manifest=json.loads(manifest_path.read_text())
    manifest['files']['context/expected.json']={'bytes':expected.stat().st_size,'sha256':sha256(expected)}
    manifest_path.write_text(json.dumps(manifest))
    result_path=target/'result.json';result=json.loads(result_path.read_text());result['manifest_sha256']=sha256(manifest_path)
    result_path.write_text(json.dumps(result))
    with pytest.raises(ValueError,match='monthly key and play_count mismatch'):verify_restored(target)


def test_source_change_during_copy_never_publishes_a_complete_backup(sample,tmp_path,monkeypatch):
    from flux.validation import snapshot_backup as module

    original=module.shutil.copytree
    def changed(source,target,*args,**kwargs):
        result=original(source,target,*args,**kwargs)
        if Path(source).name=='bronze':(Path(source)/'unexpected').write_text('new source content')
        return result
    monkeypatch.setattr(module.shutil,'copytree',changed)
    source,approval=sample
    output=tmp_path/'backup'
    with pytest.raises(ValueError,match='source advanced'):backup(source,output,PROJECT,approval)
    assert json.loads((output/'result.json').read_text())['completed'] is False
    with pytest.raises(ValueError,match='complete snapshot'):restore(output,tmp_path/'restored')


def test_schema_distinguishes_utc_instants_from_timestamp_without_timezone():
    from flux.validation.snapshot_backup import canonical_rows

    schema={'fields':[{'name':'instant','type':'timestamp'},{'name':'local','type':'timestamp_ntz'},
                      {'name':'count','type':'long'}]}
    naive=datetime(2026,10,22)
    aware=datetime(2026,10,22,9,tzinfo=timezone(timedelta(hours=9)))
    expected=[['2026-10-22T00:00:00+00:00','2026-10-22T00:00:00',9007199254740993]]
    assert canonical_rows([[naive,naive,9007199254740993]],schema)==expected
    assert canonical_rows([[aware,naive,9007199254740993]],schema)==expected
