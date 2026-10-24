import json
import os
import shutil
import threading
import time
from pathlib import Path

from flux.gold.monthly_marts import literal
from flux.validation.dbt_pilot import MARTS,SOURCES,artifacts,context,invoke,save
from flux.validation.oracle import KEYS
from flux.validation.pinning import check_pin,sha256,verify_bundle
from flux.common.execution import current, recorded, record_inputs, progress as execution_progress

ORDERS={'song_metrics':'period_start,listener_rank,song_name,song_key',
        'artist_metrics':'period_start,listener_rank,artist_name,artist_mbid',
        'artist_song_metrics':'period_start,artist_name,artist_mbid,artist_song_rank,song_name,song_key',
        'coverage':'period_start','variant_metrics':'period_start,variant',
        'listener_metrics':'period_start,listener_key','identity_coverage':'period_start,identity_status'}


def ident(name):
    return '"'+name.replace('"','""')+'"'


def separate_output(output,sources):
    output=Path(output).resolve()
    for source in sources:
        source=Path(source).resolve()
        if output==source or output in source.parents or source in output.parents:raise ValueError('overlapping output')


def compare_tables(c,actual,expected,keys):
    left=c.execute('describe select * from '+actual).fetchall()
    right=c.execute('describe select * from '+expected).fetchall()
    if [(r[0],r[1]) for r in left]!=[(r[0],r[1]) for r in right]:raise ValueError('comparison schema mismatch')
    names=[r[0] for r in left]
    if not keys or not set(keys)<=set(names):raise ValueError('invalid comparison keys')
    fields=','.join(ident(k) for k in keys)
    for relation in [actual,expected]:
        if c.execute('select 1 from '+relation+' where '+' or '.join(ident(k)+' is null' for k in keys)+' limit 1').fetchone():
            raise ValueError('null comparison key')
        if c.execute('select 1 from '+relation+' group by '+fields+' having count(*)>1 limit 1').fetchone():
            raise ValueError('duplicate comparison key')
    bad=[]
    for name,kind,*_ in left:
        a,b='a.'+ident(name),'e.'+ident(name)
        if kind in ['DOUBLE','FLOAT']:
            expr=f'case when {a} is null or {b} is null then {a} is distinct from {b} else not (isfinite({a}) and isfinite({b}) and abs({a}-{b})<=greatest(1e-12,1e-12*greatest(abs({a}),abs({b})))) end'
        else:expr=a+' is distinct from '+b
        bad.append(expr)
    matched='a.__present is not null and e.__present is not null'
    checks=[f'count(*) filter(where {matched} and ({expr}))' for expr in bad]
    values=c.execute('select count(a.__present),count(e.__present),count(*) filter(where a.__present is null),'
                     'count(*) filter(where e.__present is null),'+','.join(checks)+
                     ' from (select *,1 as __present from '+actual+') a full join (select *,1 as __present from '+expected+
                     ') e on '+' and '.join('a.'+ident(k)+'=e.'+ident(k) for k in keys)).fetchone()
    return {'matches':not any(values[2:]),'rows':values[0],'expected_rows':values[1],
            'missing':values[2],'extra':values[3],'columns':dict(zip(names,values[4:])),'float_tolerance':1e-12}


def configure_project(source,output,memory_limit='8GB'):
    import yaml

    source,output=Path(source),Path(output)
    shutil.copytree(source,output)
    path=output/'profiles.yml';profile=yaml.safe_load(path.read_text(encoding='utf-8'))
    settings=profile['flux_gold']['outputs']['pilot']
    settings['threads']=1
    settings['config_options']={'temp_directory':"{{ env_var('FLUX_DBT_TEMP_DIRECTORY') }}"}
    if memory_limit not in ['8GB','16GB']:raise ValueError('unsupported memory limit')
    settings['settings'].update(threads=2,memory_limit=memory_limit,max_temp_directory_size='100GB',
                               preserve_insertion_order=False)
    settings['settings']['enable_external_access']=settings['settings'].pop('enable_external_access')
    path.write_text(yaml.safe_dump(profile,sort_keys=False),encoding='utf-8')
    path=output/'dbt_project.yml';project=yaml.safe_load(path.read_text(encoding='utf-8'))
    project['models']['flux_gold']['intermediate']={'+materialized':'table'}
    path.write_text(yaml.safe_dump(project,sort_keys=False),encoding='utf-8')
    return {'dbt_threads':1,'engine_threads':2,'memory_limit':memory_limit,'max_temp_directory_size':'100GB',
            'intermediate_materialized':'table','external_access_in_dbt':False}


def merge_results(previous,current):
    combined={n['unique_id']:n for n in previous}
    if len(combined)!=len(previous):raise ValueError('duplicate previous nodes')
    for node in current:
        if node['unique_id'] not in combined:raise ValueError('unknown retry node')
        combined[node['unique_id']]=node
    nodes=list(combined.values())
    if len(nodes)!=169 or any(n['status'] not in ['success','pass'] for n in nodes):
        raise ValueError('unsuccessful full build nodes')
    return nodes


def connect(root,memory_limit='8GB'):
    import duckdb

    c=duckdb.connect(str(Path(root)/'pilot.duckdb'))
    if memory_limit not in ['8GB','16GB']:raise ValueError('unsupported memory limit')
    c.execute('set threads=2');c.execute('set memory_limit='+literal(memory_limit))
    c.execute("set max_temp_directory_size='100GB'");c.execute('set preserve_insertion_order=false')
    c.execute('set TimeZone=\'UTC\'')
    c.execute('set temp_directory='+literal(Path(root)/'spill'))
    return c


def profiles(c):
    result={}
    for name in SOURCES:
        fields=[r[0] for r in c.execute('describe pinned.'+name).fetchall()]
        count,xor,total=c.execute('select count(*),bit_xor(hash('+','.join(map(ident,fields))+')),sum(hash('
                                  +','.join(map(ident,fields))+')) from pinned.'+name).fetchone()
        result[name]={'rows':count,'hash_xor':str(xor),'hash_sum':str(total)}
    return result


def verify_sources(source):
    for pinned in source['tables'].values():check_pin(pinned)
    for path,digest in source['source_file_sha256'].items():
        if sha256(path)!=digest:raise ValueError('original input changed: '+path)


def prepare(baseline,root,source,reuse=None):
    gold=Path(source['source_gold'])
    prior=None
    if reuse:
        reuse=Path(reuse).resolve();separate_output(root,[reuse])
        previous=json.loads((reuse/'result.json').read_text(encoding='utf-8'))
        prior=json.loads((reuse/'inputs.json').read_text(encoding='utf-8'))
        if previous.get('completed') is not False or not previous.get('failure'):
            raise ValueError('failed, closed preparation required')
        if previous['input_manifest_sha256']!=sha256(reuse/'inputs.json') or prior['source']!=source:
            raise ValueError('reuse input changed')
        if prior['baseline_sha256']!=sha256(Path(baseline)/'result.json') or (reuse/'pilot.duckdb.wal').exists():
            raise ValueError('incompatible or unclosed preparation')
        shutil.copyfile(reuse/'pilot.duckdb',Path(root)/'pilot.duckdb')
    with connect(root) as c:
        if not prior:
            c.execute('create schema pinned')
            paths=[f['path'] for f in source['tables']['monthly_user_recordings']['files']]
            c.read_parquet(paths,hive_partitioning=True).create_view('source_monthly')
            c.execute('create table pinned.monthly as select user_name,recording_msid,listened_month,play_count from source_monthly')
            for name in ['song_dimension','song_labels','song_credits']:
                c.execute('create table pinned.'+name+' as select * from read_parquet(?)',[str(gold/(name+'.parquet'))])
            c.execute('drop view source_monthly')
        result=profiles(c)
        result['monthly']['plays']=c.execute('select sum(play_count)::bigint from pinned.monthly').fetchone()[0]
        if prior and result!=prior['profiles']:raise ValueError('reused table content changed')
        c.execute('checkpoint')
    expected=json.loads((gold/'result.json').read_text(encoding='utf-8'))
    if result['monthly']['plays']!=expected['listens']:raise ValueError('full input listens mismatch')
    info={'baseline_path':str(baseline),'baseline_sha256':sha256(Path(baseline)/'result.json'),
          'source_gold':str(gold),'source':source,'profiles':result,'production_applied':False}
    (Path(root)/'inputs.json').write_text(json.dumps(info,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return info


def compare_exports(c,actual,expected,keys,progress=None):
    dates=c.execute('select distinct period_start from read_parquet(?) union select distinct period_start from read_parquet(?) order by 1',
                    [str(actual),str(expected)]).fetchall()
    result={'matches':True,'rows':0,'expected_rows':0,'missing':0,'extra':0,'columns':{},'periods':len(dates)}
    for index,(date,) in enumerate(dates):
        if date is None:raise ValueError('null period key')
        where=' where period_start='+literal(date)+'::date'
        c.execute('create or replace temp view compared_actual as select * from read_parquet('+literal(actual)+')'+where)
        c.execute('create or replace temp view compared_expected as select * from read_parquet('+literal(expected)+')'+where)
        found=compare_tables(c,'compared_actual','compared_expected',keys)
        result['matches']&=found['matches']
        for key in ['rows','expected_rows','missing','extra']:result[key]+=found[key]
        for key,count in found['columns'].items():result['columns'][key]=result['columns'].get(key,0)+count
        if index%25==0 or index==len(dates)-1:
            measured={'periods_checked':index+1,'periods':len(dates),'rows_checked':result['rows'],
                      'expected_rows_checked':result['expected_rows'],'table':str(actual)}
            execution_progress(measured)
            if progress:progress(measured)
    return result


class DiskMeter:
    def __init__(self,root):
        self.root=Path(root);self.stopped=threading.Event();self.peak=0;self.spill_peak=0
        self.recorder=current();self.recording_error=None
        self.thread=threading.Thread(target=self.run,daemon=True)

    def sample(self):
        total=spill=0
        for path in self.root.rglob('*'):
            try:
                if path.is_file():
                    size=path.stat().st_size;total+=size
                    if 'spill' in path.relative_to(self.root).parts:spill+=size
            except OSError:pass
        self.peak=max(self.peak,total);self.spill_peak=max(self.spill_peak,spill)
        if self.recorder is not None:
            try:self.recorder.sample(dict(workspace_bytes=total,spill_bytes=spill))
            except Exception as error:self.recording_error=error;self.stopped.set()

    def run(self):
        while not self.stopped.wait(5):self.sample()

    def stop(self):
        self.stopped.set();self.thread.join();self.sample()
        if self.recording_error is not None:raise self.recording_error


def awake(enabled):
    if os.name!='nt':return None
    import ctypes

    return bool(ctypes.windll.kernel32.SetThreadExecutionState(0x80000003 if enabled else 0x80000000))


@recorded('dbt-full')
def run_full(baseline,output,project,reuse=None,resume_failed=False,memory_limit='8GB'):
    baseline,output,project=map(lambda p:Path(p).resolve(),[baseline,output,project])
    verify_bundle(baseline);scope=context(baseline)
    source=json.loads((baseline/'source_context.json').read_text(encoding='utf-8'))
    record_inputs({'silver_tables':{name:{key:table[key] for key in ['id','version'] if key in table}
                   for name,table in source['tables'].items()},'baseline_sha256':sha256(baseline/'result.json')})
    gold=Path(source['source_gold'])
    separate_output(output,[baseline,gold,Path(source['audit']),*[t['path'] for t in source['tables'].values()]])
    output.mkdir(parents=True,exist_ok=False)
    report={'completed':False,'production_applied':False,'promotion_ready':False,'context':scope,'seconds':{},'commands':{},'comparisons':{}}
    save(output,report);start=time.perf_counter();meter=DiskMeter(output);meter.thread.start()
    report['sleep_prevention']=awake(True)
    def mark(stage):
        report['stage']=stage;save(output,report);print(json.dumps({'stage':stage},ensure_ascii=False),flush=True)
    try:
        mark('verify original inputs');begin=time.perf_counter();verify_sources(source)
        report['seconds']['input_verification']=round(time.perf_counter()-begin,3)
        mark('copy full inputs');begin=time.perf_counter();info=prepare(baseline,output,source,reuse)
        report['seconds']['input_copy']=round(time.perf_counter()-begin,3);report['inputs']=info['profiles']
        report['input_manifest_sha256']=sha256(output/'inputs.json')
        snapshot=output/'project';report['configuration']=configure_project(project,snapshot,memory_limit)
        project_hashes={p.relative_to(snapshot).as_posix():sha256(p) for p in snapshot.rglob('*') if p.is_file()}
        report['project_sha256']=project_hashes
        prior_nodes=None;build_command=['build']
        if resume_failed:
            if not reuse:raise ValueError('retry requires failed preparation')
            state=Path(reuse).resolve()/'target-build'
            prior_nodes=json.loads((state/'run_results.json').read_text(encoding='utf-8'))['results']
            report['retry_state']={'path':str(state),'run_results_sha256':sha256(state/'run_results.json'),
                                   'manifest_sha256':sha256(state/'manifest.json')}
            old_report=json.loads((Path(reuse)/'result.json').read_text(encoding='utf-8'))
            old_hashes=old_report['project_sha256']
            changed={p for p in old_hashes.keys()|project_hashes.keys() if old_hashes.get(p)!=project_hashes.get(p)}
            if changed-{'profiles.yml','tests/yearly_user_contributions.sql'}:
                raise ValueError('retry model contract changed')
            report['retry_project_changes']=sorted(changed)
            build_command+=['--select','result:error+','result:fail+','result:skipped','--state',str(state)]
        for tag,command in [('debug',['debug']),('build',build_command),('docs',['docs','generate'])]:
            mark('dbt '+tag);run=invoke(output,snapshot,command,tag=tag,timeout=28800)
            report['commands'][tag]=run
            if run['returncode']!=0:raise ValueError('full dbt failed: '+tag)
            if tag=='build':
                nodes=json.loads((Path(run['target'])/'run_results.json').read_text(encoding='utf-8'))['results']
                if prior_nodes:
                    report['retry_nodes']=len(nodes);nodes=merge_results(prior_nodes,nodes)
                elif len(nodes)!=169 or any(n['status'] not in ['success','pass'] for n in nodes):
                    raise ValueError('unsuccessful full build nodes')
                report['build_nodes']={s:sum(n['status']==s for n in nodes) for s in ['success','pass']}
                report['node_seconds']={n['unique_id']:n['execution_time'] for n in nodes}
        report['documentation']=artifacts(report['commands']['docs']['target'])
        with connect(output,memory_limit) as c:
            mark('export and compare marts')
            for period,names in MARTS.items():
                (output/'marts'/period).mkdir(parents=True)
                for old,new in names.items():
                    mark('export '+new);begin=time.perf_counter();path=output/'marts'/period/(old+'.parquet')
                    c.execute('copy (select * from analytics.'+new+' order by '+ORDERS[old]+') to '+literal(path)+' (format parquet,compression zstd)')
                    report['seconds']['export_'+new]=round(time.perf_counter()-begin,3)
                    mark('compare '+new);begin=time.perf_counter()
                    def progress(data):print(json.dumps({'table':new,**data}),flush=True)
                    result=compare_exports(c,path,gold/period/(old+'.parquet'),KEYS[old],progress)
                    report['seconds']['compare_'+new]=round(time.perf_counter()-begin,3)
                    report['comparisons'][new]=result;save(output,report)
                    if not result['matches']:raise ValueError('full mart mismatch: '+new)
            mark('verify copied tables');after=profiles(c);expected=info['profiles']
            for name in SOURCES:
                if any(after[name][k]!=expected[name][k] for k in after[name]):raise ValueError('copied table changed: '+name)
            c.execute('checkpoint')
        mark('verify original inputs after run');begin=time.perf_counter();verify_sources(source);verify_bundle(baseline)
        report['seconds']['final_verification']=round(time.perf_counter()-begin,3)
        if sha256(baseline/'result.json')!=info['baseline_sha256'] or sha256(output/'inputs.json')!=report['input_manifest_sha256']:
            raise ValueError('input manifest changed')
        if project_hashes!={p.relative_to(snapshot).as_posix():sha256(p) for p in snapshot.rglob('*') if p.is_file()}:
            raise ValueError('executed project changed')
        if prior_nodes:
            state=Path(report['retry_state']['path'])
            if any(sha256(state/name)!=report['retry_state'][key] for name,key in [('run_results.json','run_results_sha256'),('manifest.json','manifest_sha256')]):
                raise ValueError('retry state changed')
        report['storage']={'database_bytes':(output/'pilot.duckdb').stat().st_size,
                           'marts_bytes':sum(p.stat().st_size for p in (output/'marts').rglob('*.parquet')),
                           'previous_marts_bytes':sum((gold/p/(n+'.parquet')).stat().st_size for p,names in MARTS.items() for n in names),
                           'marts':{p.relative_to(output).as_posix():p.stat().st_size for p in (output/'marts').rglob('*.parquet')}}
        report.update(completed=True,source_unchanged=True,copied_tables_unchanged=True,project_unchanged=True,stage='completed')
    except Exception as exc:
        report.update(completed=False,failure=type(exc).__name__+': '+str(exc)[:500])
    finally:
        awake(False);meter.stop();report['peak_workspace_bytes']=meter.peak;report['sampled_peak_spill_bytes']=meter.spill_peak
        report['elapsed_seconds']=round(time.perf_counter()-start,3);save(output,report)
    return report
