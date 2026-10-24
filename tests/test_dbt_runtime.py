from pathlib import Path

import duckdb
import pytest

pytest.importorskip('dbt.cli.main')

from test_dbt_pilot import baseline
from flux.validation.dbt_pilot import bootstrap,execute
from flux.validation.dbt_drill import drill
from flux.common.execution import inspect_run

pytestmark=pytest.mark.dbt


def test_real_dbt_preserves_semantics_and_rejects_injected_data_errors(baseline,tmp_path,monkeypatch):
    logs=tmp_path/'executions';monkeypatch.setenv('FLUX_EXECUTION_ROOT',str(logs))
    root=tmp_path/'pilot';bootstrap(baseline,root)
    result=execute(root,Path(__file__).resolve().parents[1]/'dbt/flux_gold')
    assert result['completed'] is True,result.get('failure')
    assert result['final_comparison']['plays']==27
    assert result['final_comparison']['tables_checked']==14
    assert result['documentation']['models']==24 and result['documentation']['sources']==4
    assert result['documentation']['mart_lineage_checked']==14
    assert result['build_nodes']['success']==24
    assert result['build_nodes']['pass']>140
    journals=list(logs.glob('*.sqlite3'));assert len(journals)==1
    execution=inspect_run(journals[0])
    assert execution['status']=='succeeded'
    assert execution['inputs']['context']['source_state']['version']==0
    assert execution['metrics']['final_comparison']['tables_checked']==14
    assert execution['metrics']['final_comparison']['plays']==27
    assert [e['stage'] for e in execution['events'] if e['kind']=='stage_finished']==[
        'dbt debug','dbt build','dbt rebuild','dbt docs','final comparison']
    assert len([e for e in execution['events'] if e['kind']=='progress'])==8
    with duckdb.connect(str(root/'pilot.duckdb'),read_only=True) as c:
        assert c.execute("select listens,listeners,top_user_listens from analytics.monthly_song_metrics where song_key='mbid:r' and period_start='2026-01-01'").fetchone()==(6,2,5)
        assert c.execute("select period_listens,linked_listens,period_users,linked_users from analytics.monthly_coverage where period_start='2026-01-01'").fetchone()==(20,12,3,3)
        assert c.execute("select count(*) from analytics.monthly_song_metrics where period_start='2026-01-01' and listener_rank=1").fetchone()[0]==2
        assert c.execute("select variant from analytics.monthly_song_metrics where song_key='msid:fast'").fetchone()[0]=='sped_up+remix'
        assert c.execute("select song_name from analytics.monthly_song_metrics where period_start='2026-01-01' and listener_rank=1 order by song_name,song_key").fetchall()==[('Another',),('Song',)]
        assert c.execute("select listens,listeners,top_user_listens from analytics.monthly_artist_metrics where artist_mbid='a' and period_start='2026-01-01'").fetchone()==(12,3,9)
        assert c.execute("select sum(listens) from analytics.monthly_artist_metrics where period_start='2026-01-01'").fetchone()[0]==18
        assert c.execute("select song_listen_share,song_listener_reach,artist_song_rank from analytics.monthly_artist_song_metrics where artist_mbid='a' and song_key='mbid:r' and period_start='2026-01-01'").fetchone()==(0.5,2/3,1)
        assert c.execute("select count(*) from analytics.monthly_artist_song_metrics where artist_mbid='a' and artist_song_rank=1 and period_start='2026-01-01'").fetchone()[0]==2
        assert c.execute("select listens,listeners,top_user_listens from analytics.monthly_variant_metrics where variant='unmarked' and period_start='2026-01-01'").fetchone()==(13,3,9)
        assert c.execute("select listens,song_count,top_song_share from analytics.monthly_listener_metrics where listener_key=sha256('u1') and period_start='2026-01-01'").fetchone()==(9,2,5/9)
        assert c.execute("select listens,listeners from analytics.monthly_identity_coverage where identity_status='no_mbid' and period_start='2026-01-01'").fetchone()==(8,2)
        assert c.execute("select listens,listeners,top_user_listens,top_user_share,concentration_hhi,effective_listeners from analytics.yearly_song_metrics where song_key='mbid:r' and period_start='2026-01-01'").fetchone()==pytest.approx((10,2,9,0.9,0.82,100/82))
        assert c.execute("select sum(listeners) from analytics.monthly_song_metrics where song_key='mbid:r' and year(period_start)=2026").fetchone()[0]==3
        assert c.execute("select listens,listeners,top_user_listens from analytics.yearly_artist_metrics where artist_mbid='a' and period_start='2026-01-01'").fetchone()==(16,3,13)
        assert c.execute("select song_listen_share,song_listener_reach,artist_song_rank from analytics.yearly_artist_song_metrics where artist_mbid='a' and song_key='mbid:r' and period_start='2026-01-01'").fetchone()==(10/16,2/3,1)
        assert c.execute("select listens,listeners,top_user_listens from analytics.yearly_variant_metrics where variant='unmarked' and period_start='2026-01-01'").fetchone()==(17,3,13)
        assert c.execute("select listens,song_count,top_song_share from analytics.yearly_listener_metrics where listener_key=sha256('u1') and period_start='2026-01-01'").fetchone()==(13,2,9/13)
        assert c.execute("select period_listens,period_users,linked_listens,linked_users from analytics.yearly_coverage where period_start='2026-01-01'").fetchone()==(24,3,16,3)
        assert c.execute("select listens,listeners from analytics.yearly_identity_coverage where identity_status='matched_candidate' and period_start='2026-01-01'").fetchone()==(16,3)
        assert c.execute("select listens,listeners,top_user_listens from analytics.yearly_song_metrics where song_key='mbid:r' and period_start='2027-01-01'").fetchone()==(3,1,3)
        assert c.execute("select variant from analytics.yearly_song_metrics where song_key='msid:fast'").fetchone()[0]=='sped_up+remix'
    faults=drill(root)
    assert faults['drill_passed'] is True and faults['scenarios_checked']==17
    assert faults['canonical_database_unchanged'] is True
