import json
import re
import shutil
import time
from pathlib import Path

from flux.gold.monthly_marts import literal
from flux.validation.dbt_full import DiskMeter, ORDERS, awake, compare_tables, configure_project, connect, ident, profiles, separate_output, verify_sources
from flux.validation.dbt_increment import project_hashes
from flux.validation.dbt_pilot import MARTS, SOURCES, invoke, save
from flux.validation.oracle import KEYS
from flux.validation.pinning import check_pin, pin_delta, sha256, verify_bundle
from flux.validation.silver_cdf_export import COLUMNS, NAMES, TABLE
from flux.common.execution import recorded, record_inputs, progress as execution_progress


def compare_relations(c, actual, expected, keys, period, progress=None):
    dates=c.execute('select distinct '+ident(period)+' from '+actual+' union select distinct '+ident(period)+' from '+expected+' order by 1').fetchall()
    result=dict(matches=True,rows=0,expected_rows=0,missing=0,extra=0,columns={},periods=len(dates))
    for index,(date,) in enumerate(dates):
        if date is None:raise ValueError('null comparison period')
        where=' where '+ident(period)+'='+literal(date)
        c.execute('create or replace temp view cdf_actual as select * from '+actual+where)
        c.execute('create or replace temp view cdf_expected as select * from '+expected+where)
        found=compare_tables(c,'cdf_actual','cdf_expected',keys)
        result['matches'] &= found['matches']
        for key in ['rows','expected_rows','missing','extra']:result[key]+=found[key]
        for key,count in found['columns'].items():result['columns'][key]=result['columns'].get(key,0)+count
        if index%25==0 or index==len(dates)-1:
            measured=dict(periods_checked=index+1,periods=len(dates),rows_checked=result['rows'],
                          expected_rows_checked=result['expected_rows'],table=actual)
            execution_progress(measured)
            if progress:progress(measured)
    return result


def apply_cdf(c, feed, start, end):
    if type(start) is not int or type(end) is not int or not 0<=start<=end:raise ValueError('invalid CDF range')
    cols=','.join(COLUMNS);keys=list(COLUMNS[:3]);key_sql=','.join(keys)
    actions="('insert','delete','update_preimage','update_postimage')"
    bad="_commit_version is null or _commit_version<="+str(start)+' or _commit_version>'+str(end)
    bad+=' or _change_type is null or _change_type not in '+actions
    bad+=" or user_name is null or trim(user_name)='' or recording_msid is null or trim(recording_msid)=''"
    bad+=" or listened_month is null or not regexp_full_match(listened_month,'[0-9]{4}-(0[1-9]|1[0-2])')"
    bad+=' or play_count is null or play_count<1 or play_count<>trunc(play_count)'
    if c.execute('select 1 from '+feed+' where '+bad+' limit 1').fetchone():raise ValueError('invalid CDF row')
    versions=[row[0] for row in c.execute('select distinct _commit_version from '+feed+' order by 1').fetchall()]
    months=[row[0] for row in c.execute('select distinct listened_month from '+feed+' order by 1').fetchall()]
    count=c.execute('select count(*) from '+feed).fetchone()[0]
    c.execute('begin')
    try:
        for version in versions:
            for side,types in [('removed',"('delete','update_preimage')"),('added',"('insert','update_postimage')")]:
                c.execute('create or replace temp table cdf_'+side+' as select '+cols+' from '+feed+' where _commit_version='+str(version)+' and _change_type in '+types)
                if c.execute('select 1 from cdf_'+side+' group by '+key_sql+' having count(*)>1 limit 1').fetchone():raise ValueError('duplicate CDF key')
            pre,post=c.execute('select count(*) filter(where _change_type=\'update_preimage\'),count(*) filter(where _change_type=\'update_postimage\') from '+feed+' where _commit_version='+str(version)).fetchone()
            if pre!=post:raise ValueError('unpaired update images')
            join=' and '.join('m.'+key+'=r.'+key for key in keys)
            if c.execute('select 1 from cdf_removed r left join pinned.monthly m on '+join+' where m.user_name is null or m.play_count<>r.play_count limit 1').fetchone():raise ValueError('CDF before image mismatch')
            c.execute('delete from pinned.monthly m using cdf_removed r where '+join)
            join=' and '.join('m.'+key+'=a.'+key for key in keys)
            if c.execute('select 1 from cdf_added a join pinned.monthly m on '+join+' limit 1').fetchone():raise ValueError('CDF insert key exists')
            c.execute('insert into pinned.monthly select '+cols+' from cdf_added')
        c.execute('commit')
    except Exception:
        c.execute('rollback')
        raise
    return dict(months=months,years=sorted({month[:4] for month in months}),cdf_rows=count,commits=len(versions))


def consume_cdf(c, feed, start, end, cursor):
    if type(cursor) is not int or cursor not in [start,end]:raise ValueError('CDF cursor gap or regression')
    if cursor==end:
        c.execute('create or replace temp view replay_feed as select * from '+feed+' where _commit_version>'+str(cursor))
        return apply_cdf(c,'replay_feed',end,end)
    return apply_cdf(c,feed,start,end)


def file_hashes(root):
    return {p.relative_to(root).as_posix():sha256(p) for p in sorted(Path(root).rglob('*')) if p.is_file()}


def compatible_models(base, current):
    import yaml

    old,new=project_hashes(base),project_hashes(current)
    changed=sorted(name for name in old.keys()|new.keys() if old.get(name)!=new.get(name))
    def descriptions(value):
        if isinstance(value,dict):return {key:descriptions(item) for key,item in value.items() if key!='description'}
        if isinstance(value,list):return [descriptions(item) for item in value]
        return value
    for name in changed:
        if name=='macros/period_increment.sql' and name not in old:continue
        if name in ['models/schema.yml','models/sources.yml']:
            if descriptions(yaml.safe_load((base/name).read_text(encoding='utf-8')))==descriptions(yaml.safe_load((current/name).read_text(encoding='utf-8'))):continue
        if name.startswith('models/marts/') and name.endswith('.sql') and name in old:
            period='month' if Path(name).name.startswith('monthly_') else 'year'
            prefix="{{ flux_period_config('"+period+"') }}\nselect * from (\n"
            suffix=") as period_snapshot\n{% if var('flux_incremental', false) %}\nwhere period_start in ({{ flux_dates_sql('"+period+"') }})\n{% endif %}\n"
            text=(current/name).read_text(encoding='utf-8')
            if text.startswith(prefix) and text.endswith(suffix) and text[len(prefix):-len(suffix)]==(base/name).read_text(encoding='utf-8'):continue
        raise ValueError('base model contract changed: '+name)
    return changed


def prepare_source(base, baseline, output):
    import duckdb

    base,baseline,output=map(lambda p:Path(p).resolve(),[base,baseline,output])
    prior=json.loads((base/'result.json').read_text(encoding='utf-8'))
    info=json.loads((base/'inputs.json').read_text(encoding='utf-8'))
    source=json.loads((baseline/'source_context.json').read_text(encoding='utf-8'))
    if not prior.get('completed') or not prior.get('source_unchanged') or prior['input_manifest_sha256']!=sha256(base/'inputs.json'):
        raise ValueError('completed unchanged full reference required')
    if prior.get('project_sha256')!=project_hashes(base/'project') or len(prior.get('comparisons',{}))!=14 or not all(value.get('matches') for value in prior['comparisons'].values()):
        raise ValueError('verified full models and comparisons required')
    if info['source']!=source or info['baseline_sha256']!=sha256(baseline/'result.json') or (base/'pilot.duckdb.wal').exists():raise ValueError('incompatible full reference')
    separate_output(output,[base,baseline,*[p['path'] for p in source['tables'].values()]])
    verify_bundle(baseline);verify_sources(source)
    output.mkdir(parents=True,exist_ok=False);start=time.perf_counter()
    report=dict(completed=False,production_applied=False,scope='full monthly Silver; controlled Silver-level changes; frozen song metadata and credits',base=str(base),baseline=str(baseline),source=source,base_result_sha256=sha256(base/'result.json'),base_database_sha256=sha256(base/'pilot.duckdb'),base_inputs_sha256=sha256(base/'inputs.json'))
    save(output,report)
    report['original_silver_hashes']={name:file_hashes(Path(source['tables'][name]['path'])) for name in NAMES}
    for name in NAMES:
        shutil.copytree(source['tables'][name]['path'],output/'silver'/name)
        if file_hashes(output/'silver'/name)!=report['original_silver_hashes'][name]:raise ValueError('Silver copy mismatch')
    report['before_pins']={name:pin_delta(output/'silver'/name) for name in NAMES}
    with duckdb.connect(str(base/'pilot.duckdb'),read_only=True) as c:
        c.execute('set threads=2');c.execute("set memory_limit='8GB'")
        months=c.execute('select distinct listened_month from pinned.monthly order by 1').fetchall()
        chosen=None
        for (month,) in months:
            if not '2010-01'<=month<='2015-12':continue
            chosen=c.execute('select distinct m.* from pinned.monthly m join pinned.song_dimension d using(recording_msid) join pinned.song_credits cr using(song_key) where listened_month=? order by user_name,recording_msid limit 3',[month]).fetchall()
            if len(chosen)==3:break
        if not chosen or len(chosen)!=3:raise ValueError('three officially linked controls required')
        rows=[dict(zip(COLUMNS,row)) for row in chosen]
        years={month[0][:4] for month in months}
        target=next(str(year)+'-02' for year in range(1990,2030) if str(year) not in years)
        if c.execute("select 1 from pinned.monthly where user_name='__flux_full_cdf_probe__' limit 1").fetchone():raise ValueError('probe user already present')
        changes=[dict(rows[0],action='update',new_month=rows[0]['listened_month'],new_count=rows[0]['play_count']+2),dict(rows[1],action='delete',new_month=rows[1]['listened_month'],new_count=1),dict(rows[2],action='update',new_month=target,new_count=rows[2]['play_count']),dict(rows[0],user_name='__flux_full_cdf_probe__',action='insert',new_month=rows[0]['listened_month'],new_count=1)]
        report['base_rows'],report['base_plays']=c.execute('select count(*),sum(play_count)::bigint from pinned.monthly').fetchone()
    (output/'changes.json').write_text(json.dumps(changes,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    report.update(completed=True,copy_seconds=round(time.perf_counter()-start,3),silver_bytes=sum(p.stat().st_size for p in (output/'silver').rglob('*') if p.is_file()),changes_sha256=sha256(output/'changes.json'))
    save(output,report)
    return report


def produce_probe(spark, silver, output, changes):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from flux.silver.monthly_listens import pin_snapshot, touch

    silver,output,changes=map(lambda p:Path(p).resolve(),[silver,output,changes])
    separate_output(output,[silver,changes]);before=pin_snapshot(spark,silver)
    output.mkdir(parents=True,exist_ok=False);start=time.perf_counter()
    report=dict(completed=False,production_applied=False,before_versions=before,changes_sha256=sha256(changes))
    save(output,report)
    pins={name:pin_delta(silver/name,before[name]) for name in NAMES}
    if pins[TABLE]['properties'].get('delta.enableChangeDataFeed')!='true':raise ValueError('enabled CDF required')
    edits=json.loads(changes.read_text(encoding='utf-8'))
    schema='user_name string,recording_msid string,listened_month string,play_count long,action string,new_month string,new_count long'
    frame=spark.createDataFrame([tuple(row[name] for name in ['user_name','recording_msid','listened_month','play_count','action','new_month','new_count']) for row in edits],schema)
    match=' and '.join('t.'+key+'=s.'+key for key in COLUMNS[:3])
    (DeltaTable.forPath(spark,str(silver/TABLE)).alias('t').merge(frame.alias('s'),match)
     .whenMatchedDelete("s.action='delete'")
     .whenMatchedUpdate("s.action='update'",{'play_count':'s.new_count','listened_month':'s.new_month'})
     .whenNotMatchedInsert("s.action='insert'",{name:('s.new_count' if name=='play_count' else 's.new_month' if name=='listened_month' else 's.'+name) for name in COLUMNS})
     .execute())
    touch(spark,silver/TABLE,pins[TABLE]['monthly_state'])
    after=pin_snapshot(spark,silver)
    feed=(spark.read.format('delta').option('readChangeFeed','true').option('startingVersion',before[TABLE]+1).option('endingVersion',after[TABLE]).load(str(silver/TABLE)).select(*COLUMNS,'_change_type','_commit_version'))
    actions={row['_change_type']:row['count'] for row in feed.groupBy('_change_type').count().collect()}
    if not all(actions.get(action,0)>0 for action in ['insert','delete','update_preimage','update_postimage']):raise ValueError('all CDF actions required')
    feed.coalesce(1).write.mode('errorifexists').parquet(str(output/'cdf'))
    counts={}
    for label,version in [('before',before[TABLE]),('after',after[TABLE])]:
        row=spark.read.format('delta').option('versionAsOf',version).load(str(silver/TABLE)).agg(F.count('*').alias('rows'),F.sum('play_count').alias('plays')).first()
        counts[label]=row.asDict()
    if before[TABLE]>=after[TABLE]:raise ValueError('CDF range did not advance')
    report.update(completed=True,after_versions=after,counts=counts,cdf_actions=actions,cdf_rows=sum(actions.values()),artifacts=file_hashes(output/'cdf'),elapsed_seconds=round(time.perf_counter()-start,3),spark_version=spark.version)
    save(output,report)
    return report


def monthly_view(c, name, pin):
    c.read_parquet([file['path'] for file in pin['files']],hive_partitioning=True).create_view('cdf_files')
    c.execute('create or replace temp view '+name+' as select '+','.join(COLUMNS)+' from cdf_files')


def build(root, project, impact=None):
    command=['build','--full-refresh'] if impact is None else ['build','--vars',json.dumps(dict(flux_incremental=True,flux_months=impact['months'],flux_years=impact['years']))]
    run=invoke(root,project,command,tag='build',timeout=28800)
    if run['returncode']!=0:raise ValueError('dbt build failed: '+run['log'])
    nodes=json.loads((Path(run['target'])/'run_results.json').read_text(encoding='utf-8'))['results']
    if len(nodes)!=169 or any(node['status'] not in ['success','pass'] for node in nodes):raise ValueError('unsuccessful dbt nodes')
    return dict(run,nodes={status:sum(node['status']==status for node in nodes) for status in ['success','pass']})


def recovered_models(root):
    root=Path(root)
    manifest=json.loads((root/'target-build/manifest.json').read_text(encoding='utf-8'))
    models={node['name']:node for node in manifest['nodes'].values() if node['resource_type']=='model'}
    if len(models)!=24 or sum(node['resource_type']=='test' for node in manifest['nodes'].values())!=145:
        raise ValueError('unexpected interrupted manifest')
    log=(root/'build.log').read_text(encoding='utf-8')
    if re.search(r'\b(ERROR|FAIL|WARN)\b',log):raise ValueError('unsuccessful interrupted build')
    found=re.findall(r'OK created sql (?:table|view|incremental) model analytics\.([A-Za-z0-9_]+)\s+\.+ \[OK in ([0-9.]+)s\]',log)
    if not found or len({name for name,_ in found})!=len(found) or not {name for name,_ in found}<=set(models):
        raise ValueError('untrusted completed models')
    return [dict(unique_id=models[name]['unique_id'],status='success',execution_time=float(seconds),evidence='interrupted build.log') for name,seconds in found]


def verify_executed_sql(project,nodes):
    for node in nodes:
        path=node['original_file_path']
        if path.endswith('.sql') and node['package_name']=='flux_gold':
            actual=(Path(project)/path).read_text(encoding='utf-8').strip()
            if node['raw_code'].replace('\r\n','\n').strip()!=actual:raise ValueError('executed model checksum changed')


@recorded('full-cdf-resume')
def resume_full_cdf(source, output, project):
    source,output,project=map(lambda p:Path(p).resolve(),[source,output,project])
    report=json.loads((output/'result.json').read_text(encoding='utf-8'))
    if report.get('completed') or report.get('stage')!='dbt full rebuild and tests' or (output/'cursor.json').exists():
        raise ValueError('closed interruption during full build required')
    prior=output/'interrupted-result.json'
    if prior.exists():raise ValueError('recovery already attempted')
    models=recovered_models(output/'full')
    info=json.loads((source/'result.json').read_text(encoding='utf-8'))
    probe=json.loads((source/'probe/result.json').read_text(encoding='utf-8'))
    if not info.get('completed') or not probe.get('completed') or sha256(source/'result.json')!=report['source_manifest_sha256'] or sha256(source/'probe/result.json')!=report['probe_manifest_sha256']:
        raise ValueError('recovery source changed')
    if file_hashes(source/'probe/cdf')!=probe['artifacts']:raise ValueError('CDF export changed')
    before=pin_delta(source/'silver'/TABLE,report['source_versions']['before'])
    after=pin_delta(source/'silver'/TABLE,report['source_versions']['after'])
    record_inputs({'silver_before':{k:before[k] for k in ['id','version','log_sha256']},
                   'silver_after':{k:after[k] for k in ['id','version','log_sha256']}})
    if before!=info['before_pins'][TABLE] or before['id']!=after['id']:raise ValueError('recovery source history changed')
    base=Path(info['base'])
    for name,key in [('pilot.duckdb','base_database_sha256'),('result.json','base_result_sha256'),('inputs.json','base_inputs_sha256')]:
        if sha256(base/name)!=info[key]:raise ValueError('recovery base changed')
    check=output/('recovery-project-'+str(time.time_ns()))
    configure_project(project,check,'16GB')
    snapshot=project_hashes(check)
    if any(snapshot!=project_hashes(output/label/'project') for label in ['increment','full']):raise ValueError('recovery model project changed')
    manifest=json.loads((output/'full/target-build/manifest.json').read_text(encoding='utf-8'))
    verify_executed_sql(output/'full/project',manifest['nodes'].values())
    increment=json.loads((output/'increment/target-build/run_results.json').read_text(encoding='utf-8'))['results']
    if len(increment)!=169 or any(node['status'] not in ['success','pass'] for node in increment):raise ValueError('unfinished increment build')
    if not report.get('ending_input',{}).get('matches') or report.get('repeat_same_cursor',{}).get('cdf_rows')!=0:raise ValueError('unverified CDF input')
    verify_sources(info['source'])
    shutil.copyfile(output/'result.json',prior)
    start=time.perf_counter();meter=DiskMeter(output);meter.thread.start();code=project_hashes(project)
    report['recovery']=dict(reused_models=len(models),tests_rerun=145,prior_result_sha256=sha256(prior),model_evidence=models,interruption_cause='unknown',continuous_full_build_seconds=None)
    report['timing_scope']='recovery session only; prior completed phase measurements retained separately'
    report['peak_storage_scope']='recovery session only; original run peak unavailable'
    def timed(stage,action):
        report['stage']=stage;save(output,report);print(json.dumps(dict(stage=stage),ensure_ascii=False),flush=True)
        begin=time.perf_counter();value=action();report['seconds'][stage]=round(time.perf_counter()-begin,3);save(output,report);return value
    try:
        report['sleep_prevention']=awake(True)
        with connect(output/'full','16GB') as c:
            monthly_view(c,'silver_after',after)
            found=timed('recovery full input comparison',lambda:compare_relations(c,'pinned.monthly','silver_after',SOURCES['monthly'],'listened_month'))
            if not found['matches']:raise ValueError('recovered input differs from ending Silver')
            report['recovery']['full_input']=found
            c.execute('checkpoint')
        missing=sorted(node['name'] for node in manifest['nodes'].values() if node['resource_type']=='model' and node['unique_id'] not in {model['unique_id'] for model in models})
        report['recovery']['rebuilt_models']=missing
        run=timed('recovery full dbt build and tests',lambda:invoke(output/'full',output/'full/project',['build','--full-refresh','--select','resource_type:test',*missing],tag='recovery-build',timeout=28800))
        nodes=json.loads((Path(run['target'])/'run_results.json').read_text(encoding='utf-8'))['results']
        combined=models+nodes
        if run['returncode']!=0 or len(combined)!=169 or len({node['unique_id'] for node in combined})!=169 or any(node['status'] not in ['pass','success'] for node in combined):raise ValueError('unsuccessful recovery build')
        report['full_build']=dict(run,recovered=True,nodes={'success':24,'pass':145})
        finish_full_cdf(source,output,project,info,probe,report,before,after,code,timed)
    except Exception as error:
        report.update(completed=False,failure=type(error).__name__+': '+str(error))
    finally:
        awake(False);meter.stop();report['peak_workspace_bytes']=meter.peak;report['sampled_peak_spill_bytes']=meter.spill_peak
        report['elapsed_seconds']=round(time.perf_counter()-start,3);save(output,report)
    return report


@recorded('full-cdf-gold')
def run_full_cdf(source, output, project):
    source,output,project=map(lambda p:Path(p).resolve(),[source,output,project])
    info=json.loads((source/'result.json').read_text(encoding='utf-8'))
    probe=json.loads((source/'probe/result.json').read_text(encoding='utf-8'))
    if not info.get('completed') or not probe.get('completed') or info['changes_sha256']!=sha256(source/'changes.json') or info['changes_sha256']!=probe['changes_sha256']:raise ValueError('completed source and CDF required')
    if file_hashes(source/'probe/cdf')!=probe['artifacts']:raise ValueError('CDF export changed')
    base=Path(info['base']);baseline=Path(info['baseline'])
    base_report=json.loads((base/'result.json').read_text(encoding='utf-8'))
    separate_output(output,[source,base,baseline,*[p['path'] for p in info['source']['tables'].values()],project])
    for name in NAMES:check_pin(info['before_pins'][name]) if name!=TABLE else None
    before=pin_delta(source/'silver'/TABLE,probe['before_versions'][TABLE])
    after=pin_delta(source/'silver'/TABLE,probe['after_versions'][TABLE])
    record_inputs({'silver_before':{k:before[k] for k in ['id','version','log_sha256']},
                   'silver_after':{k:after[k] for k in ['id','version','log_sha256']},
                   'silver_table_versions':{'before':probe['before_versions'],'after':probe['after_versions']}})
    if before!=info['before_pins'][TABLE] or before['id']!=after['id']:raise ValueError('source history changed')
    if sha256(base/'pilot.duckdb')!=info['base_database_sha256'] or sha256(base/'result.json')!=info['base_result_sha256'] or sha256(base/'inputs.json')!=info['base_inputs_sha256']:raise ValueError('base snapshot changed')
    output.mkdir(parents=True,exist_ok=False);start=time.perf_counter();meter=DiskMeter(output);meter.thread.start()
    report=dict(completed=False,production_applied=False,promotion_ready=False,scope=info['scope'],source_manifest_sha256=sha256(source/'result.json'),probe_manifest_sha256=sha256(source/'probe/result.json'),seconds={},comparisons={},source_versions=dict(before=before['version'],after=after['version']),source_id=before['id'],cdf_actions=probe['cdf_actions'],base_rows=info['base_rows'],base_plays=info['base_plays'],sleep_prevention=awake(True))
    save(output,report)
    code=project_hashes(project)
    def mark(stage):
        report['stage']=stage;save(output,report);print(json.dumps(dict(stage=stage),ensure_ascii=False),flush=True)
    def timed(stage,action):
        mark(stage);begin=time.perf_counter();value=action();report['seconds'][stage]=round(time.perf_counter()-begin,3);save(output,report);return value
    try:
        timed('verify originals',lambda:verify_sources(info['source']))
        for label in ['increment','full']:
            root=output/label;root.mkdir()
            timed('copy '+label,lambda:shutil.copyfile(base/'pilot.duckdb',root/'pilot.duckdb'))
            configure_project(project,root/'project','16GB')
            if project_hashes(base/'project')!=base_report['project_sha256']:raise ValueError('base project changed')
            report['compatible_model_upgrade']=compatible_models(base/'project',root/'project')
            (root/'inputs.json').write_text(json.dumps(dict(baseline_sha256=sha256(source/'probe/result.json')),indent=2)+'\n',encoding='utf-8')
        with connect(output/'increment','16GB') as c:
            monthly_view(c,'silver_before',before)
            found=timed('check full starting input',lambda:compare_relations(c,'pinned.monthly','silver_before',SOURCES['monthly'],'listened_month'))
            report['starting_input']=found
            if not found['matches']:raise ValueError('base Gold input differs from Silver start')
            c.read_parquet(str(source/'probe/cdf/*.parquet')).create_view('feed')
            impact=timed('apply CDF',lambda:consume_cdf(c,'feed',before['version'],after['version'],before['version']));report['impact']=impact
            replay=timed('replay same CDF at ending cursor',lambda:consume_cdf(c,'feed',before['version'],after['version'],after['version']))
            if replay!=dict(months=[],years=[],cdf_rows=0,commits=0):raise ValueError('same range replay was not empty')
            report['repeat_same_cursor']=dict(replay,dbt_invoked=False,cursor_before=after['version'],cursor_after=after['version'])
            monthly_view(c,'silver_after',after)
            found=timed('check full ending input',lambda:compare_relations(c,'pinned.monthly','silver_after',SOURCES['monthly'],'listened_month'))
            report['ending_input']=found
            if not found['matches']:raise ValueError('CDF result differs from Silver end')
            c.execute('checkpoint')
        with connect(output/'full','16GB') as c:
            monthly_view(c,'silver_after',after)
            timed('load independent ending snapshot',lambda:c.execute('create or replace table pinned.monthly as select * from silver_after'))
            c.execute('checkpoint')
        report['increment_build']=timed('dbt increment build and tests',lambda:build(output/'increment',output/'increment/project',impact))
        report['full_build']=timed('dbt full rebuild and tests',lambda:build(output/'full',output/'full/project'))
        finish_full_cdf(source,output,project,info,probe,report,before,after,code,timed)
    except Exception as error:
        report.update(completed=False,failure=type(error).__name__+': '+str(error))
    finally:
        awake(False);meter.stop();report['peak_workspace_bytes']=meter.peak;report['sampled_peak_spill_bytes']=meter.spill_peak;report['elapsed_seconds']=round(time.perf_counter()-start,3);save(output,report)
    return report


def finish_full_cdf(source,output,project,info,probe,report,before,after,code,timed):
    base=Path(info['base']);impact=report['impact']
    for label in ['increment','full']:
        with connect(output/label,'16GB') as c:
            report[label+'_inputs']=timed(label+' input profiles',lambda:profiles(c))
            for period,names in MARTS.items():
                (output/label/'marts'/period).mkdir(parents=True)
                for old,new in names.items():
                    path=output/label/'marts'/period/(old+'.parquet')
                    timed(label+' export '+new,lambda:c.execute('copy (select * from analytics.'+new+' order by '+ORDERS[old]+') to '+literal(path)+' (format parquet,compression zstd)'))
            c.execute('checkpoint')
    if report['increment_inputs']!=report['full_inputs']:raise ValueError('source profiles differ')
    with connect(output/'increment','16GB') as c:
        for period,names in MARTS.items():
            for old,new in names.items():
                left=output/'increment/marts'/period/(old+'.parquet');right=output/'full/marts'/period/(old+'.parquet')
                c.execute('create or replace temp view final_actual as select * from read_parquet('+literal(left)+')')
                c.execute('create or replace temp view final_expected as select * from read_parquet('+literal(right)+')')
                found=timed('compare '+new,lambda:compare_relations(c,'final_actual','final_expected',KEYS[old],'period_start'))
                report['comparisons'][new]=found
                if not found['matches']:raise ValueError('full mart mismatch: '+new)
                values=impact['months'] if period=='month' else impact['years']
                excluded=','.join(literal(value+('-01' if period=='month' else '-01-01'))+'::date' for value in values)
                old_path=base/'marts'/period/(old+'.parquet')
                c.execute('create or replace temp view untouched_actual as select * from final_actual where period_start not in ('+excluded+')')
                c.execute('create or replace temp view untouched_expected as select * from read_parquet('+literal(old_path)+') where period_start not in ('+excluded+')')
                untouched=timed('untouched '+new,lambda:compare_relations(c,'untouched_actual','untouched_expected',KEYS[old],'period_start'))
                report.setdefault('untouched',{})[new]=untouched
                if not untouched['matches']:raise ValueError('unaffected period changed: '+new)
    timed('verify originals after run',lambda:verify_sources(info['source']))
    if any(file_hashes(Path(info['source']['tables'][name]['path']))!=info['original_silver_hashes'][name] for name in NAMES):raise ValueError('original Silver files changed')
    if sha256(base/'pilot.duckdb')!=info['base_database_sha256']:raise ValueError('original base DB changed')
    if code!=project_hashes(project):raise ValueError('model project changed')
    for name in NAMES:check_pin(pin_delta(source/'silver'/name,probe['after_versions'][name]))
    if sha256(source/'result.json')!=report['source_manifest_sha256'] or sha256(source/'probe/result.json')!=report['probe_manifest_sha256'] or file_hashes(source/'probe/cdf')!=probe['artifacts']:raise ValueError('source export changed')
    report['storage']={label:dict(database_bytes=(output/label/'pilot.duckdb').stat().st_size,marts_bytes=sum(p.stat().st_size for p in (output/label/'marts').rglob('*.parquet'))) for label in ['increment','full']}
    report['storage']['silver_before_bytes']=sum(file['bytes'] for file in before['files'])
    report['storage']['silver_after_active_bytes']=sum(file['bytes'] for file in after['files'])
    report['storage']['silver_with_history_bytes']=sum(p.stat().st_size for p in (source/'silver').rglob('*') if p.is_file())
    report['storage']['cdf_bytes']=sum(p.stat().st_size for p in (source/'probe/cdf').rglob('*.parquet'))
    report['mart_sha256']={label:file_hashes(output/label/'marts') for label in ['increment','full']}
    (output/'cursor.json').write_text(json.dumps(dict(source=str(source/'silver'),source_id=after['id'],version=after['version'],log_sha256=after['log_sha256'],project_sha256=code),indent=2)+'\n',encoding='utf-8')
    report.update(completed=True,source_unchanged=True,base_unchanged=True,model_unchanged=True,stage='completed',cursor_sha256=sha256(output/'cursor.json'))
