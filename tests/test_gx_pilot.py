import json

import duckdb
import pytest

from test_dbt_pilot import baseline,pin,populate_expected
from test_quality_contract import dataset
from flux.validation import gx_pilot
from flux.validation.pinning import sha256
from flux.validation.dbt_pilot import bootstrap
from flux.gold.monthly_marts import literal


def fake_engine(data,root):
    return {'completed':True,'passed':True,'checks':[{'rule_id':name,'success':True} for name in ['DQ003','DQ004','DQ005']]}


def test_sample_runner_preserves_bundle_and_blocks_modified_input(baseline,tmp_path,monkeypatch):
    before=sha256(baseline/'result.json')
    monkeypatch.setattr(gx_pilot,'load_baseline',lambda root:dataset())
    monkeypatch.setattr(gx_pilot,'engine',fake_engine)
    result=gx_pilot.run(baseline,tmp_path/'good')
    assert result['completed'] is True and result['baseline_unchanged'] is True
    assert sha256(baseline/'result.json')==before
    def changed(data,root):
        (baseline/'inputs/monthly.parquet').write_bytes(b'changed')
        return fake_engine(data,root)
    monkeypatch.setattr(gx_pilot,'engine',changed)
    result=gx_pilot.run(baseline,tmp_path/'changed')
    assert result['completed'] is False and result['baseline_unchanged'] is False


@pytest.mark.parametrize('name,count',[('too_large',100001),('monthly',10001)])
def test_large_input_is_rejected_before_loading_python_records(baseline,tmp_path,monkeypatch,name,count):
    with duckdb.connect() as connection:
        connection.execute('copy (select * from range(?)) to '+literal(baseline/'inputs'/(name+'.parquet'))+' (format parquet)',[count])
    pin(baseline)
    def unexpected(root):raise AssertionError('large input reached Python')
    monkeypatch.setattr(gx_pilot,'load_baseline',unexpected)
    with pytest.raises(ValueError,match='bounded.*sample'):gx_pilot.run(baseline,tmp_path/'out')
    assert not (tmp_path/'out').exists()


def test_overlap_and_existing_output_are_refused(baseline,tmp_path):
    with pytest.raises(ValueError):gx_pilot.run(baseline,baseline/'nested')
    output=tmp_path/'exists';output.mkdir()
    with pytest.raises(FileExistsError):gx_pilot.validate(dataset(),output)


def test_candidate_checks_both_manifest_levels_and_preserves_database(baseline,tmp_path,monkeypatch):
    candidate=tmp_path/'dbt';bootstrap(baseline,candidate);populate_expected(candidate)
    marker=candidate/'result.json';info=json.loads(marker.read_text());info.update(completed=True,inputs_unchanged=True)
    marker.write_text(json.dumps(info));before=sha256(candidate/'pilot.duckdb')
    monkeypatch.setattr(gx_pilot,'load_baseline',lambda root:dataset())
    monkeypatch.setattr(gx_pilot,'engine',fake_engine)
    result=gx_pilot.run(baseline,tmp_path/'out',candidate_db=candidate/'pilot.duckdb')
    assert result['candidate_kind']=='dbt snapshot' and result['baseline_unchanged'] is True
    assert sha256(candidate/'pilot.duckdb')==before
    (candidate/'inputs.json').write_text('{}')
    with pytest.raises(ValueError,match='input differs'):
        gx_pilot.run(baseline,tmp_path/'bad',candidate_db=candidate/'pilot.duckdb')
