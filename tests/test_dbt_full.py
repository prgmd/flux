from pathlib import Path

import duckdb
import pytest

from flux.validation.dbt_full import compare_exports,compare_tables,configure_project,merge_results,prepare,separate_output
from flux.validation.pinning import sha256


def tables(c):
    c.execute("create table expected(k varchar, n bigint, ratio double, label varchar)")
    c.execute("insert into expected values ('one',9007199254740993,0.5,'곡'),('two',2,NULL,NULL)")
    c.execute('create table actual as select * from expected')


def test_sql_comparison_checks_exact_integers_and_nullable_values():
    with duckdb.connect() as c:
        tables(c)
        result=compare_tables(c,'actual','expected',['k'])
        assert result['matches'] and result['rows']==2
        c.execute("update actual set n=n-1 where k='one'")
        result=compare_tables(c,'actual','expected',['k'])
        assert not result['matches'] and result['columns']['n']==1


def test_equal_counts_do_not_hide_missing_and_extra_keys():
    with duckdb.connect() as c:
        tables(c);c.execute("update actual set k='other' where k='two'")
        result=compare_tables(c,'actual','expected',['k'])
        assert not result['matches'] and result['missing']==1 and result['extra']==1


@pytest.mark.parametrize('statement',[
    "insert into actual select * from actual where k='one'",
    "update actual set k=NULL where k='one'",
])
def test_duplicate_or_null_keys_are_rejected(statement):
    with duckdb.connect() as c:
        tables(c);c.execute(statement)
        with pytest.raises(ValueError,match='key'):compare_tables(c,'actual','expected',['k'])


def test_float_tolerance_and_nonfinite_values():
    with duckdb.connect() as c:
        tables(c);c.execute("update actual set ratio=0.5+1e-13 where k='one'")
        assert compare_tables(c,'actual','expected',['k'])['matches']
        for value in ['0.5+1e-7',"'NaN'::double","'Infinity'::double",'NULL']:
            c.execute("update actual set ratio="+value+" where k='one'")
            assert not compare_tables(c,'actual','expected',['k'])['matches']


def test_null_and_schema_changes_are_detected():
    with duckdb.connect() as c:
        tables(c);c.execute("update actual set label='present' where k='two'")
        assert compare_tables(c,'actual','expected',['k'])['columns']['label']==1
        c.execute('alter table actual alter n type double')
        with pytest.raises(ValueError,match='schema'):compare_tables(c,'actual','expected',['k'])


def test_output_cannot_overlap_any_source(tmp_path):
    source=tmp_path/'source'
    for output in [source,source/'child',tmp_path]:
        with pytest.raises(ValueError,match='overlap'):separate_output(output,[source])
    separate_output(tmp_path/'new',[source])


def test_full_profile_only_changes_the_copied_project(tmp_path):
    original=Path(__file__).resolve().parents[1]/'dbt/flux_gold'
    before={p.relative_to(original).as_posix():sha256(p) for p in original.rglob('*') if p.is_file()}
    result=configure_project(original,tmp_path/'project')
    assert result['dbt_threads']==1 and result['engine_threads']==2
    assert result['memory_limit']=='8GB' and result['intermediate_materialized']=='table'
    assert before=={p.relative_to(original).as_posix():sha256(p) for p in original.rglob('*') if p.is_file()}
    assert (tmp_path/'project/profiles.yml').read_text(encoding='utf-8').count('8GB')==1
    import yaml

    profile=yaml.safe_load((tmp_path/'project/profiles.yml').read_text(encoding='utf-8'))['flux_gold']['outputs']['pilot']
    assert 'temp_directory' in profile['config_options'] and 'temp_directory' not in profile['settings']


def test_partitioned_comparison_keeps_periods_and_tied_ranks(tmp_path):
    left,right=tmp_path/'left.parquet',tmp_path/'right.parquet'
    with duckdb.connect() as c:
        c.execute("create table t as select * from(values (date '2026-01-01','s',1),(date '2027-01-01','s',1)) t(period_start,song_key,listener_rank)")
        c.execute('copy t to ? (format parquet)',[str(left)])
        c.execute('copy t to ? (format parquet)',[str(right)])
        found=compare_exports(c,left,right,['period_start','song_key'])
        assert found['matches'] and found['periods']==2 and found['rows']==2
        c.execute("update t set listener_rank=2 where period_start='2027-01-01'")
        c.execute('copy t to ? (format parquet)',[str(left)])
        found=compare_exports(c,left,right,['period_start','song_key'])
        assert not found['matches'] and found['columns']['listener_rank']==1


def test_active_or_tampered_preparation_cannot_be_reused(tmp_path):
    import json

    previous=tmp_path/'previous';previous.mkdir()
    (previous/'result.json').write_text(json.dumps({'completed':False}),encoding='utf-8')
    (previous/'inputs.json').write_text('{}',encoding='utf-8')
    source={'source_gold':str(tmp_path/'gold')}
    with pytest.raises(ValueError,match='closed'):prepare(tmp_path/'baseline',tmp_path/'output',source,previous)
    (previous/'result.json').write_text(json.dumps({'completed':False,'failure':'debug failed','input_manifest_sha256':'bad'}),encoding='utf-8')
    with pytest.raises(ValueError,match='changed'):prepare(tmp_path/'baseline',tmp_path/'output',source,previous)
    assert not (tmp_path/'output').exists()


def test_retry_requires_success_for_every_previously_failed_or_skipped_node():
    nodes=[{'unique_id':str(i),'status':'success' if i<24 else 'pass'} for i in range(169)]
    nodes[24]['status']='error';nodes[25]['status']='skipped'
    with pytest.raises(ValueError):merge_results(nodes,[{'unique_id':'24','status':'pass'}])
    found=merge_results(nodes,[{'unique_id':'24','status':'pass'},{'unique_id':'25','status':'pass'}])
    assert len(found)==169 and all(n['status'] in ['success','pass'] for n in found)
    with pytest.raises(ValueError):merge_results(nodes,[{'unique_id':'unknown','status':'pass'}])
