from pathlib import Path

import duckdb
import pytest

pytest.importorskip('dbt.cli.main')

from test_dbt_pilot import baseline
from flux.validation.dbt_pilot import bootstrap,execute
from flux.validation.dbt_drill import drill

pytestmark=pytest.mark.dbt


def test_real_dbt_preserves_semantics_and_rejects_injected_data_errors(baseline,tmp_path):
    root=tmp_path/'pilot';bootstrap(baseline,root)
    result=execute(root,Path(__file__).resolve().parents[1]/'dbt/flux_gold')
    assert result['completed'] is True,result.get('failure')
    assert result['final_comparison']['plays']==18
    assert result['documentation']['models']==7 and result['documentation']['sources']==4
    assert result['build_nodes']=={'success':7,'pass':37}
    with duckdb.connect(str(root/'pilot.duckdb'),read_only=True) as c:
        assert c.execute("select listens,listeners,top_user_listens from analytics.monthly_song_metrics where song_key='mbid:r' and period_start='2026-01-01'").fetchone()==(6,2,5)
        assert c.execute("select period_listens,linked_listens,period_users,linked_users from analytics.monthly_coverage where period_start='2026-01-01'").fetchone()==(14,6,3,2)
        assert c.execute("select count(*) from analytics.monthly_song_metrics where period_start='2026-01-01' and listener_rank=2").fetchone()[0]==2
        assert c.execute("select variant from analytics.monthly_song_metrics where song_key='msid:fast'").fetchone()[0]=='sped_up+remix'
        assert c.execute("select song_name from analytics.monthly_song_metrics where period_start='2026-01-01' and listener_rank=2 order by song_name,song_key").fetchall()==[('Song (sped up remix)',),('Unknown',)]
    faults=drill(root)
    assert faults['drill_passed'] is True and faults['scenarios_checked']==5
    assert faults['canonical_database_unchanged'] is True
