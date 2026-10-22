import json
import shutil
from pathlib import Path

from flux.validation.dbt_pilot import compare_snapshot,invoke
from flux.validation.pinning import sha256


def drill(root):
    import duckdb

    root=Path(root).resolve();original=json.loads((root/'result.json').read_text(encoding='utf-8'))
    if original.get('completed') is not True:raise ValueError('completed pilot required')
    info=json.loads((root/'inputs.json').read_text(encoding='utf-8'))
    directory=root/'faults';directory.mkdir(exist_ok=False)
    before=sha256(root/'pilot.duckdb');results=[]
    cases=[('duplicate_monthly',"insert into pinned.monthly select * from pinned.monthly limit 1",'build','unique_grain_stg_monthly'),
           ('zero_count',"update pinned.monthly set play_count=0 where rowid=(select min(rowid) from pinned.monthly)",'build','positive_integer_stg_monthly_play_count'),
           ('missing_dimension',"delete from pinned.song_dimension where recording_msid=(select recording_msid from pinned.monthly limit 1)",'build','relationships_stg_monthly_recording_msid'),
           ('wrong_rank',"update analytics.monthly_song_metrics set listener_rank=999999",'test','monthly_joint_ranks'),
           ('wrong_coverage',"update analytics.monthly_coverage set linked_listens=period_listens+1",'test','monthly_coverage_bounds')]
    for name,sql,command,expected in cases:
        output=directory/name;output.mkdir()
        shutil.copyfile(root/'pilot.duckdb',output/'pilot.duckdb')
        (output/'inputs.json').write_text(json.dumps({'baseline_sha256':info['baseline_sha256'],'fault_injection':True}),encoding='utf-8')
        with duckdb.connect(str(output/'pilot.duckdb')) as c:c.execute(sql)
        run=invoke(output,root/'project',[command],tag='check')
        path=Path(run['target'])/'run_results.json'
        nodes=json.loads(path.read_text(encoding='utf-8'))['results'] if path.is_file() else []
        failed=[r['unique_id'] for r in nodes if r['status']=='fail']
        errors=[r['unique_id'] for r in nodes if r['status']=='error']
        detected=run['returncode']==1 and any(expected in uid for uid in failed) and not errors
        results.append({'scenario':name,'expected_test':expected,'detected':detected,'returncode':run['returncode'],
                        'failed_tests':failed,'errors':errors,'seconds':run['seconds']})
    check=compare_snapshot(root)
    unchanged=before==sha256(root/'pilot.duckdb')
    result={'drill_passed':all(r['detected'] for r in results) and unchanged,'scenarios_checked':len(results),
            'canonical_database_unchanged':unchanged,'canonical_comparison':check,'scenarios':results,'production_applied':False}
    (root/'drill-result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return result
