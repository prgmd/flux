import json
from pathlib import Path

import duckdb
import pytest

from flux.gold.monthly_marts import build_dimension,build_period
from flux.validation.dbt_pilot import MARTS,bootstrap,compare_snapshot,execute
from flux.validation.oracle import KEYS
from flux.validation.pinning import sha256


@pytest.fixture
def baseline(tmp_path):
    root=tmp_path/'baseline';(root/'inputs').mkdir(parents=True);(root/'expected/month').mkdir(parents=True)
    (root/'source_manifests').mkdir()
    with duckdb.connect() as c:
        c.execute("""create table reviewed as select * from(values
            ('s1','r','Song','Artist','Song','', 'matched_candidate'),
            ('s2','r','Song','Artist','Song','', 'matched_candidate'),
            ('fast',NULL,'Song (sped up remix)','Artist',NULL,NULL,'no_mbid'),
            ('unknown',NULL,'Unknown','Other',NULL,NULL,'no_mbid'),
            ('s3','r3','Another','Artist','Another','','matched_candidate'))
            t(recording_msid,recording_mbid,title,artist,recording_title,comment,status)""")
        c.execute("create table proposed_credits as select * from(values ('s1','a','Artist'),('s1','b','Guest'),('s2','a','Artist'),('s2','b','Guest'),('s3','a','Artist')) t(recording_msid,artist_mbid,artist_name)")
        c.execute("""create table monthly as select * from(values
            ('u1','s1','2026-01',2),('u1','s2','2026-01',3),('u2','s1','2026-01',1),
            ('u1','s1','2026-02',4),('u2','fast','2026-01',7),('u3','unknown','2026-01',1),
            ('u1','s3','2026-01',4),('u3','s3','2026-01',2))
            t(user_name,recording_msid,listened_month,play_count)""")
        build_dimension(c)
        for name in ['monthly','song_dimension','song_labels','song_credits']:
            c.execute('copy '+name+' to ? (format parquet)',[str(root/'inputs'/(name+'.parquet'))])
        build_period(c,'month')
        for name in KEYS:
            c.execute('copy '+name+' to ? (format parquet)',[str(root/'expected/month'/(name+'.parquet'))])
    state={'source_id':'fixture','version':0,'minimum_ms_played':30000,'policy_version':1,
           'timezone':'UTC','key':['user_name','recording_msid','listened_month']}
    context={'r9_reference_only':True,'gold_policies':{'timezone':'UTC'},'tables':{'bronze':{'id':'fixture','version':0},
             **{n:{'monthly_state':state} for n in ['monthly_user_recordings','recording_metadata','invalid_listens']}}}
    for name,value in {'source_context.json':context,'compatibility.json':{'listener_key':'user_name'},
                       'source_manifests/r9_production_export.json':{'approved_candidates':0,'mappings':[]}}.items():
        (root/name).write_text(json.dumps(value),encoding='utf-8')
    pin(root)
    return root


def pin(root):
    artifacts={p.relative_to(root).as_posix():sha256(p) for p in root.rglob('*') if p.is_file() and p.name!='result.json'}
    (root/'result.json').write_text(json.dumps({'completed':True,'artifacts':artifacts}),encoding='utf-8')


def populate_expected(root):
    with duckdb.connect(str(root/'pilot.duckdb')) as c:
        c.execute('create schema analytics')
        for old,new in MARTS.items():
            c.execute('create table analytics.'+new+' as select * from read_parquet(?)',[str(root/'expected'/(old+'.parquet'))])


def test_bootstrap_copies_only_pinned_inputs_into_a_separate_database(baseline,tmp_path):
    root=tmp_path/"pilot's output"
    info=bootstrap(baseline,root)
    assert info['prepared'] is True and info['production_applied'] is False
    with duckdb.connect(str(root/'pilot.duckdb')) as c:
        assert c.execute('select count(*),sum(play_count) from pinned.monthly').fetchone()==(8,24)
    assert sha256(root/'inputs/monthly.parquet')==sha256(baseline/'inputs/monthly.parquet')
    with pytest.raises(FileExistsError):bootstrap(baseline,root)
    with pytest.raises(ValueError):bootstrap(baseline,baseline/'nested')


def test_missing_or_tampered_baseline_never_creates_an_output(baseline,tmp_path):
    (baseline/'inputs/monthly.parquet').write_bytes(b'changed')
    with pytest.raises(ValueError):bootstrap(baseline,tmp_path/'output')
    assert not (tmp_path/'output').exists()


@pytest.mark.parametrize('field,value',[('listener_key','user_id'),('minimum_ms_played',10000),('approved_candidates',False)])
def test_incompatible_context_is_rejected_before_writes(baseline,tmp_path,field,value):
    if field=='listener_key':path=baseline/'compatibility.json'
    elif field=='minimum_ms_played':path=baseline/'source_context.json'
    else:path=baseline/'source_manifests/r9_production_export.json'
    content=json.loads(path.read_text())
    if field=='minimum_ms_played':content['tables']['monthly_user_recordings']['monthly_state'][field]=value
    else:content[field]=value
    path.write_text(json.dumps(content));pin(baseline)
    with pytest.raises(ValueError):bootstrap(baseline,tmp_path/'output')
    assert not (tmp_path/'output').exists()


def test_frozen_and_python_expectations_detect_changed_rank_and_source(baseline,tmp_path):
    root=tmp_path/'output';bootstrap(baseline,root);populate_expected(root)
    result=compare_snapshot(root)
    assert result['tables_checked']==7 and result['mismatches']==0
    with duckdb.connect(str(root/'pilot.duckdb')) as c:c.execute('update analytics.monthly_song_metrics set listener_rank=999')
    with pytest.raises(ValueError,match='listener_rank'):compare_snapshot(root)
    with duckdb.connect(str(root/'pilot.duckdb')) as c:
        c.execute('delete from pinned.song_dimension')
    with pytest.raises(ValueError,match='source'):compare_snapshot(root)


def test_all_seven_monthly_marts_are_copied_and_compared(baseline,tmp_path):
    assert set(MARTS)==set(KEYS)
    root=tmp_path/'output';bootstrap(baseline,root);populate_expected(root)
    assert len(list((root/'expected').glob('*.parquet')))==7
    result=compare_snapshot(root)
    assert result['tables_checked']==7 and result['plays']==24


@pytest.mark.parametrize('mart,column,value',[
    ('artist_metrics','top_user_listens','1'),
    ('artist_song_metrics','song_listen_share','0.99'),
    ('variant_metrics','listeners','999'),
    ('listener_metrics','song_count','999'),
    ('identity_coverage','listens','999'),
])
def test_new_marts_are_compared_by_every_key_and_value(baseline,tmp_path,mart,column,value):
    root=tmp_path/'output';bootstrap(baseline,root);populate_expected(root)
    with duckdb.connect(str(root/'pilot.duckdb')) as c:
        c.execute('update analytics.monthly_'+mart+' set '+column+'='+value)
    with pytest.raises(ValueError,match=column):compare_snapshot(root)


def test_failed_dbt_execution_cannot_publish_a_completed_result(baseline,tmp_path,monkeypatch):
    import flux.validation.dbt_pilot as module

    root=tmp_path/'output';bootstrap(baseline,root)
    monkeypatch.setattr(module,'invoke',lambda *a,**k:{'returncode':1,'seconds':0})
    result=execute(root,Path(__file__).resolve().parents[1]/'dbt/flux_gold')
    assert result['completed'] is False and result['production_applied'] is False
    assert json.loads((root/'result.json').read_text())['completed'] is False


def test_invoke_routes_generated_artifacts_outside_project(baseline,tmp_path,monkeypatch):
    from types import SimpleNamespace
    import flux.validation.dbt_pilot as module

    root=tmp_path/'output';bootstrap(baseline,root)
    calls=[]
    def process(args,**kwargs):
        calls.append((args,kwargs));return SimpleNamespace(returncode=0)
    monkeypatch.setattr(module.subprocess,'run',process)
    module.invoke(root,tmp_path/'project',['build'])
    args,kwargs=calls[0]
    assert Path(args[args.index('--target-path')+1])==root/'target-build'
    assert args.index('--target-path')>args.index('build')
    assert Path(args[args.index('--log-path')+1])==root/'logs-build'
    assert kwargs['env']['DBT_SEND_ANONYMOUS_USAGE_STATS']=='false'
