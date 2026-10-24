import json
from pathlib import Path

import pytest

pytest.importorskip('deltalake')
import pyarrow as pa
from deltalake import DeltaTable, CommitProperties, write_deltalake

from flux.validation.snapshot_backup import backup, restore, verify_delta, TABLES


pytestmark=pytest.mark.delta_restore


def test_actual_delta_restores_without_source_and_reads_history_cdf_and_empty_table(tmp_path,monkeypatch):
    monkeypatch.setenv('FLUX_EXECUTION_ROOT',str(tmp_path/'executions'))
    source=tmp_path/'source'
    write_deltalake(source/'bronze',pa.table({'raw_json':['{}']}),configuration={'delta.enableChangeDataFeed':'true'})
    state=dict(source_id=DeltaTable(source/'bronze').metadata().id,version=0,
               key=['user_name','recording_msid','listened_month'],minimum_ms_played=30000,policy_version=1,timezone='UTC')
    commit=CommitProperties(custom_metadata={'userMetadata':json.dumps({'monthly_silver':state})})
    for name,relative in TABLES.items():
        if name=='bronze':continue
        if name=='monthly_user_recordings':
            data=pa.table({'user_name':['u'],'recording_msid':['s'],'listened_month':['2026-01'],'play_count':pa.array([1],type=pa.int64())})
        elif name=='recording_metadata':data=pa.table({'recording_msid':['s'],'track_name':['Song']})
        else:data=pa.table({'reason':pa.array([],type=pa.string())})
        write_deltalake(source/relative,data,configuration={'delta.enableChangeDataFeed':'true'},commit_properties=commit)
        if name=='monthly_user_recordings':
            data=data.set_column(3,'play_count',pa.array([2],type=pa.int64()))
            write_deltalake(source/relative,data,mode='overwrite',commit_properties=commit)
    approval=tmp_path/'approval.json'
    approval.write_text(json.dumps(dict(mode='production',approved_candidates=0,human_approved_candidates=0,mappings=[])))
    saved=tmp_path/'backup';target=tmp_path/'restored'
    assert backup(source,saved,Path(__file__).resolve().parents[1],approval)['completed']
    source.rename(tmp_path/'offline-source')
    assert restore(saved,target)['completed']
    result=verify_delta(target,tmp_path/'verification')
    assert result['completed'] and result['source_paths_used'] is False
    assert result['comparison']['plays']==2 and result['comparison']['key_mismatches']==0
    assert result['tables']['monthly_user_recordings']['history_versions']==2
    assert result['tables']['monthly_user_recordings']['version_zero_rows']==1
    assert result['tables']['invalid_listens']['rows']==0
    assert all(table['active_paths_local'] for table in result['tables'].values())
    assert result['tables']['monthly_user_recordings']['cdf_actions']=={'insert':2,'delete':1}
