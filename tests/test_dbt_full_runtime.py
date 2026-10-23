from pathlib import Path
import json

import pytest

pytest.importorskip('dbt.cli.main')

from test_dbt_pilot import baseline
from flux.validation.dbt_full import compare_exports,configure_project,connect,merge_results
from flux.validation.dbt_pilot import MARTS,bootstrap,execute,invoke
from flux.validation.oracle import KEYS

pytestmark=pytest.mark.dbt


def test_full_materialization_profile_keeps_all_fourteen_sample_results(baseline,tmp_path):
    project=tmp_path/'full-project'
    configure_project(Path(__file__).resolve().parents[1]/'dbt/flux_gold',project)
    root=tmp_path/'output';bootstrap(baseline,root)
    result=execute(root,project)
    assert result['completed'],result.get('failure')
    assert result['build_nodes']=={'success':24,'pass':145}
    with connect(root) as c:
        assert c.execute("select count(*) from information_schema.tables where table_schema='analytics' and table_type='BASE TABLE'").fetchone()[0]==20
        for period,names in MARTS.items():
            for old,new in names.items():
                target=root/(new+'.parquet')
                c.execute('copy (select * from analytics.'+new+' order by period_start) to ? (format parquet,compression zstd)',[str(target)])
                found=compare_exports(c,target,root/'expected'/period/(old+'.parquet'),KEYS[old])
                assert found['matches'] and found['missing']==found['extra']==0


def test_failed_test_and_skipped_models_resume_without_rebuilding_successful_models(baseline,tmp_path):
    original=Path(__file__).resolve().parents[1]/'dbt/flux_gold'
    bad=tmp_path/'bad-project';configure_project(original,bad)
    (bad/'tests/yearly_user_contributions.sql').write_text("select * from {{ ref('int_user_yearly_songs') }} where missing_retry_probe",encoding='utf-8')
    root=tmp_path/'output';bootstrap(baseline,root)
    result=execute(root,bad)
    assert not result['completed']
    state=root/'target-build'
    before=json.loads((state/'run_results.json').read_text(encoding='utf-8'))['results']
    assert any(n['status']=='error' for n in before) and any(n['status']=='skipped' for n in before)
    fixed=tmp_path/'fixed-project';configure_project(original,fixed,'16GB')
    run=invoke(root,fixed,['build','--select','result:error+','result:fail+','result:skipped','--state',str(state)],tag='retry')
    assert run['returncode']==0
    current=json.loads((Path(run['target'])/'run_results.json').read_text(encoding='utf-8'))['results']
    combined=merge_results(before,current)
    assert len(current)<169 and sum(n['status']=='success' for n in combined)==24
    assert sum(n['status']=='pass' for n in combined)==145
    with connect(root) as c:
        c.execute('insert into analytics.int_user_yearly_songs select * from analytics.int_user_yearly_songs limit 1')
    fault=invoke(root,fixed,['test','--select','int_user_yearly_songs'],tag='duplicate-year')
    results=json.loads((Path(fault['target'])/'run_results.json').read_text(encoding='utf-8'))['results']
    assert fault['returncode']==1 and not any(n['status']=='error' for n in results)
    assert any(n['status']=='fail' and 'unique_grain_int_user_yearly_songs' in n['unique_id'] for n in results)
