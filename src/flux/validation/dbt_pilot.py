import json
import os
import shutil
import subprocess
import sys
import time
from importlib.metadata import PackageNotFoundError,version
from pathlib import Path

from flux.gold.monthly_marts import build_period,literal
from flux.validation.baseline import rows
from flux.validation.oracle import KEYS,compare_rows,python_marts
from flux.validation.pinning import sha256,verify_bundle

SOURCES={'monthly':['user_name','recording_msid','listened_month'],'song_dimension':['recording_msid'],
         'song_labels':['song_key'],'song_credits':['song_key','artist_mbid']}
MARTS={name:'monthly_'+name for name in KEYS}


def save(root,result):
    root=Path(root);temp=root/'result.tmp'
    temp.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    temp.replace(root/'result.json')


def context(baseline):
    source=json.loads((baseline/'source_context.json').read_text(encoding='utf-8'))
    compat=json.loads((baseline/'compatibility.json').read_text(encoding='utf-8'))
    approval=json.loads((baseline/'source_manifests/r9_production_export.json').read_text(encoding='utf-8'))
    states=[source['tables'][n]['monthly_state'] for n in ['monthly_user_recordings','recording_metadata','invalid_listens']]
    state=states[0];bronze=source['tables']['bronze']
    ok=(compat['listener_key']=='user_name' and source['gold_policies']['timezone']=='UTC'
        and source['r9_reference_only'] is True and type(approval['approved_candidates']) is int
        and approval['approved_candidates']==0 and approval['mappings']==[]
        and all(s==state for s in states) and state['source_id']==bronze['id'] and state['version']==bronze['version']
        and type(state['version']) is int and state['version']>=0 and type(state['minimum_ms_played']) is int
        and state['minimum_ms_played']==30000 and state['policy_version']==1 and state['timezone']=='UTC'
        and state['key']==['user_name','recording_msid','listened_month'])
    if not ok:raise ValueError('incompatible pinned context')
    return {'listener_key':'user_name','timezone':'UTC','minimum_ms_played':30000,'source_state':state,
            'r9_reference_only':True,'approved_mappings':0}


def bootstrap(baseline,output):
    import duckdb

    baseline,output=Path(baseline).resolve(),Path(output).resolve()
    if output==baseline or output in baseline.parents or baseline in output.parents:raise ValueError('overlapping baseline')
    verify_bundle(baseline);scope=context(baseline);before=sha256(baseline/'result.json')
    output.mkdir(parents=True,exist_ok=False);(output/'inputs').mkdir();(output/'expected').mkdir()
    for name in SOURCES:shutil.copyfile(baseline/'inputs'/(name+'.parquet'),output/'inputs'/(name+'.parquet'))
    for name in MARTS:shutil.copyfile(baseline/'expected/month'/(name+'.parquet'),output/'expected'/(name+'.parquet'))
    with duckdb.connect(str(output/'pilot.duckdb')) as c:
        c.execute('set threads=2');c.execute("set memory_limit='2GB'")
        c.execute('create schema pinned')
        for name in SOURCES:c.execute('create table pinned.'+name+' as select * from read_parquet(?)',[str(output/'inputs'/(name+'.parquet'))])
    verify_bundle(baseline)
    if before!=sha256(baseline/'result.json'):raise ValueError('baseline changed during bootstrap')
    info={'prepared':True,'production_applied':False,'scope':'fixed monthly sample, seven monthly marts only',
          'baseline_path':str(baseline),'baseline_sha256':before,'context':scope,
          'artifacts':{p.relative_to(output).as_posix():sha256(p) for d in ['inputs','expected'] for p in sorted((output/d).iterdir())}}
    (output/'inputs.json').write_text(json.dumps(info,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    save(output,{'completed':False,'production_applied':False,'input_manifest_sha256':sha256(output/'inputs.json')})
    return info


def verify_inputs(root):
    root=Path(root).resolve();info=json.loads((root/'inputs.json').read_text(encoding='utf-8'))
    for name,digest in info['artifacts'].items():
        path=(root/name).resolve()
        if root not in path.parents or sha256(path)!=digest:raise ValueError('copied input changed')
    return info


def compare_snapshot(root):
    import duckdb

    root=Path(root);verify_inputs(root)
    with duckdb.connect(str(root/'pilot.duckdb')) as c:
        c.execute('set threads=2');c.execute("set memory_limit='2GB'")
        saved={}
        for name,keys in SOURCES.items():
            saved[name]=rows(c,'read_parquet('+literal(root/'inputs'/(name+'.parquet'))+')')
            try:compare_rows(saved[name],rows(c,'pinned.'+name),keys)
            except ValueError as exc:raise ValueError('pinned source changed: '+name) from exc
        expected=python_marts(saved['monthly'],saved['song_dimension'],saved['song_labels'],saved['song_credits'],'month')
        with duckdb.connect() as legacy:
            legacy.execute('set threads=2')
            for name in SOURCES:
                legacy.execute('create table '+name+' as select * from read_parquet(?)',[str(root/'inputs'/(name+'.parquet'))])
            build_period(legacy,'month')
            counts={}
            for old,new in MARTS.items():
                actual=rows(c,'analytics.'+new)
                frozen=rows(c,'read_parquet('+literal(root/'expected'/(old+'.parquet'))+')')
                for wanted in [frozen,expected[old],rows(legacy,old)]:compare_rows(wanted,actual,KEYS[old])
                actual_schema=c.execute('describe analytics.'+new).fetchall()
                frozen_schema=c.execute('describe select * from read_parquet(?)',[str(root/'expected'/(old+'.parquet'))]).fetchall()
                if [(r[0],r[1]) for r in actual_schema]!=[(r[0],r[1]) for r in frozen_schema]:raise ValueError('mart column/type mismatch')
                counts[new]=len(actual)
        return {'tables_checked':len(MARTS),'mismatches':0,'checks_against':['frozen P0','Python oracle','legacy SQL'],
                'table_rows':counts,'plays':sum(r['play_count'] for r in saved['monthly']),
                'source_rows':len(saved['monthly']),'source_tables_unchanged':True,'float_tolerance':1e-12}


def invoke(root,project,command,*,tag=None):
    root,project=Path(root).resolve(),Path(project).resolve()
    info=json.loads((root/'inputs.json').read_text(encoding='utf-8'))
    tag=tag or '-'.join(command)
    target=root/('target-'+tag);logs=root/('logs-'+tag)
    env={**os.environ,'FLUX_DBT_DATABASE':(root/'pilot.duckdb').as_posix(),
         'FLUX_DBT_BASELINE_SHA256':info['baseline_sha256'],'DBT_SEND_ANONYMOUS_USAGE_STATS':'false',
         'DBT_PARTIAL_PARSE':'false','DBT_USE_COLORS':'false','PYTHONIOENCODING':'utf-8'}
    args=[sys.executable,'-c','from dbt.cli.main import cli; cli()',
          '--log-path',str(logs),*command,
          '--project-dir',str(project),'--profiles-dir',str(project)]
    if command[0]!='debug':args+=['--target-path',str(target)]
    begin=time.perf_counter()
    with (root/(tag+'.log')).open('w',encoding='utf-8') as stream:
        process=subprocess.run(args,env=env,cwd=root,stdout=stream,stderr=subprocess.STDOUT,timeout=180)
    return {'returncode':process.returncode,'seconds':round(time.perf_counter()-begin,3),'target':str(target),'log':tag+'.log'}


def artifacts(target):
    target=Path(target)
    manifest=json.loads((target/'manifest.json').read_text(encoding='utf-8'))
    catalog=json.loads((target/'catalog.json').read_text(encoding='utf-8'))
    required={'monthly_song_metrics':{'int_user_monthly_songs','stg_song_labels'},
              'monthly_coverage':{'int_user_monthly_songs','stg_song_credits'},
              'monthly_artist_metrics':{'int_user_monthly_artists','stg_song_credits'},
              'monthly_artist_song_metrics':{'monthly_song_metrics','stg_song_credits','monthly_artist_metrics'},
              'monthly_variant_metrics':{'int_user_monthly_variants','monthly_coverage'},
              'monthly_listener_metrics':{'int_user_monthly_songs'},
              'monthly_identity_coverage':{'int_user_monthly_songs','stg_song_labels'}}
    for name,parents in required.items():
        node='model.flux_gold.'+name
        actual=set(manifest['nodes'][node]['depends_on']['nodes'])
        if actual!={'model.flux_gold.'+p for p in parents}:raise ValueError('lineage mismatch')
        columns=manifest['nodes'][node]['columns']
        if set(columns)!=set(catalog['nodes'][node]['columns']) or not all(c['description'] for c in columns.values()):
            raise ValueError('missing catalog column descriptions')
    if not (target/'index.html').is_file():raise ValueError('docs index missing')
    models=[n for n in manifest['nodes'].values() if n['resource_type']=='model']
    return {'models':len(models),'sources':len(manifest['sources']),'mart_lineage_checked':len(required),
            'catalog_models':len(catalog['nodes']),'docs_generated':True}


def execute(root,project):
    root,project=Path(root).resolve(),Path(project).resolve()
    report=json.loads((root/'result.json').read_text(encoding='utf-8'))
    report.update(completed=False,production_applied=False,commands={})
    save(root,report)
    try:
        info=verify_inputs(root)
        if report['input_manifest_sha256']!=sha256(root/'inputs.json'):raise ValueError('input manifest changed')
        snapshot=root/'project';shutil.copytree(project,snapshot)
        source_hashes={p.relative_to(snapshot).as_posix():sha256(p) for p in snapshot.rglob('*') if p.is_file()}
        report['project_sha256']=source_hashes
        for name in ['dbt-core','dbt-duckdb','duckdb']:
            try:report.setdefault('runtime',{})[name]=version(name)
            except PackageNotFoundError:report.setdefault('runtime',{})[name]=None
        report['runtime']['python']=sys.version.split()[0]
        for tag,command in [('debug',['debug']),('build',['build']),('rebuild',['build']),('docs',['docs','generate'])]:
            stage=invoke(root,snapshot,command,tag=tag);report['commands'][tag]=stage;save(root,report)
            if stage['returncode']!=0:raise ValueError('dbt command failed: '+tag)
            if tag in ('build','rebuild'):
                checks=json.loads((Path(stage['target'])/'run_results.json').read_text(encoding='utf-8'))
                if not checks['results'] or any(r['status'] not in ['success','pass'] for r in checks['results']):raise ValueError('unsuccessful dbt build nodes')
                report[tag+'_nodes']={s:sum(r['status']==s for r in checks['results']) for s in ['success','pass']}
                report[tag+'_comparison']=compare_snapshot(root)
        report['documentation']=artifacts(report['commands']['docs']['target'])
        report['final_comparison']=compare_snapshot(root)
        verify_inputs(root);verify_bundle(Path(info['baseline_path']))
        if sha256(Path(info['baseline_path'])/'result.json')!=info['baseline_sha256']:raise ValueError('original baseline changed')
        if source_hashes!={p.relative_to(snapshot).as_posix():sha256(p) for p in snapshot.rglob('*') if p.is_file()}:raise ValueError('executed project changed')
        report.update(completed=True,baseline_unchanged=True,inputs_unchanged=True,project_unchanged=True,
                      promotion_ready=False,scope=info['scope'])
    except (ValueError,OSError,KeyError,subprocess.SubprocessError) as exc:
        report.update(completed=False,error_type=type(exc).__name__,failure=str(exc)[:250])
    save(root,report)
    return report
