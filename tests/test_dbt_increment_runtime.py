import json
import os
import shutil
import subprocess
import sys
from copy import deepcopy
from pathlib import Path

import duckdb
import pytest

pytest.importorskip('dbt.cli.main')

from test_dbt_pilot import baseline
from flux.validation.dbt_pilot import bootstrap,execute
from flux.validation.dbt_increment import load_sources,update
from flux.validation.pinning import sha256

pytestmark=pytest.mark.dbt
PROJECT=Path(__file__).resolve().parents[1]/'dbt/flux_gold'


def test_real_period_replacement_replays_deletes_and_recovers_without_changing_previous(baseline,tmp_path):
    initial=tmp_path/'initial';bootstrap(baseline,initial)
    assert execute(initial,PROJECT)['completed'] is True
    original=sha256(initial/'pilot.duckdb')
    data=load_sources(initial)
    data['monthly'][0]['play_count']+=1
    first=tmp_path/'first';result=update(initial,first,data,PROJECT)
    assert result['completed'] is True,result.get('failure')
    assert result['impact']['months']==['2026-01']
    assert result['untouched_periods_match'] is True
    assert result['checks']['tables_checked']==14
    assert result['build_nodes']=={'success':24,'pass':145}
    assert sha256(initial/'pilot.duckdb')==original
    replay=tmp_path/'replay';again=update(first,replay,data,PROJECT)
    assert again['completed'] is True and again['no_changes'] is True
    assert 'build' not in again['commands']
    deleted=deepcopy(data);deleted['monthly']=[row for row in deleted['monthly'] if row['listened_month']!='2027-01']
    second=tmp_path/'second';result=update(first,second,deleted,PROJECT)
    assert result['completed'] is True,result.get('failure')
    with duckdb.connect(str(second/'pilot.duckdb'),read_only=True) as connection:
        assert connection.execute("select count(*) from analytics.yearly_song_metrics where period_start='2027-01-01'").fetchone()[0]==0
        assert connection.execute("select listens,listeners from analytics.yearly_song_metrics where period_start='2026-01-01' and song_key='mbid:r'").fetchone()==(11,2)
    bad=tmp_path/'bad-project';shutil.copytree(PROJECT,bad)
    model=bad/'models/marts/monthly_song_metrics.sql';model.write_text(model.read_text().replace('sum(plays)::bigint','sum(missing_column)::bigint',1))
    changed=deepcopy(deleted);changed['monthly'][0]['play_count']+=1
    failed=update(second,tmp_path/'failed',changed,bad,allow_full_rebuild=True)
    assert failed['completed'] is False
    with pytest.raises(ValueError):update(tmp_path/'failed',tmp_path/'forbidden',changed,PROJECT)
    recovered=update(second,tmp_path/'recovered',changed,PROJECT)
    assert recovered['completed'] is True,recovered.get('failure')
    assert recovered['checks']['plays']==sum(row['play_count'] for row in changed['monthly'])
    snapshot=tmp_path/'next.json';snapshot.write_text(json.dumps(changed))
    script=PROJECT.parents[1]/'scripts/update_dbt_gold.py'
    process=subprocess.run([sys.executable,str(script),'--previous',str(second),'--next-snapshot',str(snapshot),
        '--output',str(tmp_path/'crash'),'--project',str(PROJECT),'--crash-after-build'],
        env={**os.environ,'PYTHONPATH':str(PROJECT.parents[1]/'src')},capture_output=True,text=True)
    assert process.returncode==86,process.stdout[-1500:]+process.stderr[-1500:]
    assert json.loads((tmp_path/'crash/result.json').read_text())['completed'] is False
    fixed=update(second,tmp_path/'after-crash',changed,PROJECT)
    assert fixed['completed'] is True
