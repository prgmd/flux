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
           ('wrong_coverage',"update analytics.monthly_coverage set linked_listens=period_listens+1",'test','monthly_coverage_bounds'),
           ('wrong_artist',"update analytics.monthly_artist_metrics set top_user_listens=top_user_listens+1",'test','monthly_artist_reconciliation'),
           ('wrong_artist_song',"update analytics.monthly_artist_song_metrics set song_listen_share=0",'test','monthly_artist_song_reconciliation'),
           ('wrong_variant',"update analytics.monthly_variant_metrics set listen_share=0",'test','monthly_variant_reconciliation'),
           ('wrong_listener',"update analytics.monthly_listener_metrics set song_count=song_count+1",'test','monthly_listener_reconciliation'),
           ('wrong_identity',"update analytics.monthly_identity_coverage set listens=listens+1",'test','monthly_identity_reconciliation'),
           ('yearly_sum_monthly_listeners',"""update analytics.yearly_song_metrics a set listeners=(
                select sum(m.listeners) from analytics.monthly_song_metrics m
                where date_trunc('year',m.period_start)=a.period_start and m.song_key=a.song_key)""",'test','yearly_listener_counts'),
           ('wrong_yearly_artist',"update analytics.yearly_artist_metrics set top_user_listens=top_user_listens+1",'test','yearly_artist_reconciliation'),
           ('wrong_yearly_artist_song',"update analytics.yearly_artist_song_metrics set song_listen_share=0",'test','yearly_artist_song_reconciliation'),
           ('wrong_yearly_variant',"update analytics.yearly_variant_metrics set listener_reach=0",'test','yearly_variant_reconciliation'),
           ('wrong_yearly_listener',"update analytics.yearly_listener_metrics set song_count=song_count+1",'test','yearly_listener_reconciliation'),
           ('yearly_sum_monthly_users',"""update analytics.yearly_coverage a set period_users=(
                select sum(m.period_users) from analytics.monthly_coverage m
                where date_trunc('year',m.period_start)=a.period_start)""",'test','yearly_listener_counts'),
           ('wrong_yearly_identity',"update analytics.yearly_identity_coverage set listeners=listeners+1",'test','yearly_identity_reconciliation')]
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
