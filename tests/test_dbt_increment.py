import json
from copy import deepcopy
from pathlib import Path

import duckdb
import pytest

from test_dbt_pilot import baseline,populate_expected
from flux.validation.dbt_pilot import bootstrap
from flux.validation.dbt_increment import KIND,load_sources,update
from flux.validation.pinning import sha256
from flux.validation.dbt_period_drill import metadata_policy_drill

PROJECT=Path(__file__).resolve().parents[1]/'dbt/flux_gold'


@pytest.fixture
def previous(baseline,tmp_path):
    root=tmp_path/'previous';bootstrap(baseline,root);populate_expected(root)
    report=json.loads((root/'result.json').read_text());report.update(completed=True,inputs_unchanged=True)
    (root/'result.json').write_text(json.dumps(report))
    return root


def test_same_snapshot_needs_no_dbt_and_preserves_every_mart(previous,tmp_path):
    before=sha256(previous/'pilot.duckdb')
    result=update(previous,tmp_path/"same's snapshot",load_sources(previous),PROJECT)
    assert result['completed'] is True,result.get('failure')
    assert result['no_changes'] is True and result['commands']=={}
    assert result['checks']['tables_checked']==14 and result['checks']['legacy_full_sql_tables']==14
    assert result['checks']['untouched_rows_checked']>0
    assert sha256(previous/'pilot.duckdb')==before
    assert result['previous_snapshot']['database_sha256']==before


def test_unapproved_policy_change_and_incomplete_previous_are_refused(previous,tmp_path):
    data=load_sources(previous)
    with pytest.raises(ValueError,match='full rebuild'):update(previous,tmp_path/'policy',data,PROJECT,policy='different')
    report=json.loads((previous/'result.json').read_text());report['completed']=False
    (previous/'result.json').write_text(json.dumps(report))
    with pytest.raises(ValueError,match='completed'):update(previous,tmp_path/'incomplete',data,PROJECT)


def test_overlapping_or_mutated_previous_and_invalid_sources_are_refused(previous,tmp_path):
    data=load_sources(previous)
    with pytest.raises(ValueError,match='separate'):update(previous,previous/'nested',data,PROJECT)
    bad=deepcopy(data);bad['monthly'].append(deepcopy(bad['monthly'][0]))
    with pytest.raises(ValueError,match='duplicate'):update(previous,tmp_path/'duplicate',bad,PROJECT)
    (previous/'inputs/monthly.parquet').write_bytes(b'changed')
    with pytest.raises(ValueError):update(previous,tmp_path/'changed',data,PROJECT)


def test_large_source_cannot_be_collected_into_python(tmp_path):
    root=tmp_path/'large';root.mkdir()
    with duckdb.connect(str(root/'pilot.duckdb')) as connection:
        connection.execute('create schema pinned')
        connection.execute('create table pinned.monthly as select * from range(10001)')
    with pytest.raises(ValueError,match='bounded'):load_sources(root)


def test_changed_previous_database_is_rejected_even_when_original_input_files_match(previous,tmp_path):
    data=load_sources(previous)
    with duckdb.connect(str(previous/'pilot.duckdb')) as connection:
        connection.execute('update pinned.monthly set play_count=play_count+1')
    with pytest.raises(ValueError):update(previous,tmp_path/'wrong-previous',data,PROJECT)


def test_metadata_drill_refuses_overlapping_output_before_writes(previous):
    with pytest.raises(ValueError,match='separate'):metadata_policy_drill(previous,previous/'nested',PROJECT)
    assert not (previous/'nested').exists()
