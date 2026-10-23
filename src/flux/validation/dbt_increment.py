import json
import os
import shutil
import time
from collections import Counter
from pathlib import Path

from flux.gold.monthly_marts import build_period,literal
from flux.validation.baseline import rows
from flux.validation.dbt_pilot import MARTS,compare_snapshot,invoke,verify_inputs
from flux.validation.gold_changes import affected_periods
from flux.validation.oracle import KEYS,compare_rows,python_marts
from flux.validation.pinning import sha256
from flux.validation.quality_gate import verify_artifacts

SOURCES={'monthly':'monthly','dimension':'song_dimension','labels':'song_labels','credits':'song_credits'}
KIND='gold-period-increment-v1'


def save(root,value):
    (Path(root)/'result.json').write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False,default=str)+'\n',encoding='utf-8')


def project_hashes(root):
    return {path.relative_to(root).as_posix():sha256(path) for path in Path(root).rglob('*') if path.is_file()}


def load_sources(root):
    import duckdb
    with duckdb.connect(str(Path(root)/'pilot.duckdb'),read_only=True) as connection:
        result={}
        for name,table in SOURCES.items():
            count=connection.execute('select count(*) from pinned.'+table).fetchone()[0]
            if count>(10000 if name=='monthly' else 100000):raise ValueError('bounded incremental prototype required')
            result[name]=rows(connection,'pinned.'+table)
    return result


def previous_proof(root):
    report=json.loads((root/'result.json').read_text(encoding='utf-8'))
    if report.get('completed') is not True:raise ValueError('completed previous snapshot required')
    if report.get('kind')==KIND:verify_artifacts(root,report.get('artifacts'))
    else:
        if report.get('inputs_unchanged') is not True:raise ValueError('verified previous dbt inputs required')
        verify_inputs(root)
        if report.get('input_manifest_sha256')!=sha256(root/'inputs.json'):raise ValueError('previous input manifest changed')
        load_sources(root)
        import duckdb
        with duckdb.connect(str(root/'pilot.duckdb'),read_only=True) as connection:
            if any(connection.execute('select count(*) from analytics.'+table).fetchone()[0]>100000 for names in MARTS.values() for table in names.values()):
                raise ValueError('bounded previous marts required')
        compare_snapshot(root)
    return report


def verify_complete(root,data,previous,impact):
    import duckdb
    checked=untouched=0
    with duckdb.connect() as connection:
        connection.execute('set threads=2');connection.execute("set memory_limit='2GB'")
        connection.execute('attach '+literal(root/'pilot.duckdb')+' as current_snapshot (read_only)')
        connection.execute('attach '+literal(previous/'pilot.duckdb')+' as previous_snapshot (read_only)')
        for name,table in SOURCES.items():connection.execute('create view '+table+' as select * from current_snapshot.pinned.'+table)
        for period,names in MARTS.items():
            expected=python_marts(data['monthly'],data['dimension'],data['labels'],data['credits'],period)
            build_period(connection,period)
            affected={value+('-01' if period=='month' else '-01-01') for value in impact['months' if period=='month' else 'years']}
            for name,table in names.items():
                actual=rows(connection,'current_snapshot.analytics.'+table)
                compare_rows(expected[name],actual,KEYS[name])
                compare_rows(rows(connection,name),actual,KEYS[name]);checked+=1
                old=[row for row in rows(connection,'previous_snapshot.analytics.'+table) if str(row['period_start']) not in affected]
                new=[row for row in actual if str(row['period_start']) not in affected]
                compare_rows(old,new,KEYS[name]);untouched+=len(old)
    return {'tables_checked':checked,'python_oracle_tables':checked,'legacy_full_sql_tables':checked,
            'untouched_rows_checked':untouched,'plays':sum(row['play_count'] for row in data['monthly'])}


def update(previous,output,next_data,project,*,policy=None,allow_full_rebuild=False,crash_after_build=False):
    import duckdb

    previous,output,project=map(lambda value:Path(value).resolve(),[previous,output,project])
    if (previous==output or previous in output.parents or output in previous.parents
            or project==output or project in output.parents or output in project.parents):raise ValueError('separate output required')
    old_report=previous_proof(previous);old=load_sources(previous)
    data=json.loads(json.dumps({name:next_data[name] for name in SOURCES},ensure_ascii=False,allow_nan=False))
    if len(data['monthly'])>10000 or any(len(data[name])>100000 for name in SOURCES):raise ValueError('bounded next snapshot required')
    old_policy=old_report.get('policy_fingerprint','monthly-gold-policies-v1');policy=old_policy if policy is None else policy
    impact=affected_periods(old,data,policy_before=old_policy,policy_after=policy)
    code=project_hashes(project)
    code_changed=old_report.get('kind')==KIND and old_report.get('project_sha256')!=code
    if code_changed:
        impact['requires_full_rebuild']=True
        impact['months']=sorted({row['listened_month'] for source in [old,data] for row in source['monthly']})
        impact['years']=sorted({value[:4] for value in impact['months']})
    if impact['requires_full_rebuild'] and not allow_full_rebuild:raise ValueError('model or policy changed; explicit full rebuild required')
    before_db=sha256(previous/'pilot.duckdb');before_marker=sha256(previous/'result.json')
    output.mkdir(parents=True,exist_ok=False);start=time.perf_counter()
    report={'completed':False,'kind':KIND,'production_applied':False,'promotion_ready':False,'commands':{},'impact':impact,
            'policy_fingerprint':policy,'scope':'bounded controlled snapshot prototype; not a Silver CDF reader',
            'previous_snapshot':{'path':str(previous),'result_sha256':before_marker,'database_sha256':before_db}}
    save(output,report)
    try:
        shutil.copyfile(previous/'pilot.duckdb',output/'pilot.duckdb')
        snapshot=output/'project';shutil.copytree(project,snapshot)
        (output/'next-snapshot.json').write_text(json.dumps(data,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
        context_hash=sha256(output/'next-snapshot.json')
        (output/'inputs.json').write_text(json.dumps({'baseline_sha256':context_hash,'scope':report['scope']},indent=2)+'\n',encoding='utf-8')
        source_files=output/'source-inputs';source_files.mkdir()
        with duckdb.connect(str(output/'pilot.duckdb')) as connection:
            connection.execute('begin')
            for name,table in SOURCES.items():
                schema=connection.execute('describe pinned.'+table).fetchall();columns=[row[0] for row in schema]
                if any(set(row)!=set(columns) for row in data[name]):raise ValueError('input schema changed')
                file=source_files/(table+'.json');file.write_text(json.dumps(data[name],ensure_ascii=False,allow_nan=False)+'\n',encoding='utf-8')
                types='{'+','.join(literal(row[0])+':'+literal(row[1]) for row in schema)+'}'
                connection.execute('delete from pinned.'+table)
                connection.execute('insert into pinned.'+table+" select * from read_json(?,columns="+types+",format='array')",[str(file)])
            connection.execute('commit')
        no_changes=not impact['months'] and not impact['requires_full_rebuild']
        report['no_changes']=no_changes;report['project_sha256']=code;save(output,report)
        if not no_changes:
            command=['build']
            if impact['requires_full_rebuild']:command+=['--full-refresh']
            else:command+=['--vars',json.dumps({'flux_incremental':True,'flux_months':impact['months'],'flux_years':impact['years']})]
            stage=invoke(output,snapshot,command,tag='increment');report['commands']['build']=stage;save(output,report)
            if stage['returncode']!=0:raise ValueError('incremental dbt build failed')
            results=json.loads((Path(stage['target'])/'run_results.json').read_text(encoding='utf-8'))['results']
            manifest=json.loads((Path(stage['target'])/'manifest.json').read_text(encoding='utf-8'))
            expected={key for key,node in manifest['nodes'].items() if node['resource_type'] in {'model','test'}}
            delivered=[row['unique_id'] for row in results]
            if len(delivered)!=len(expected) or set(delivered)!=expected or any(row['status'] not in {'success','pass'} for row in results):
                raise ValueError('incomplete dbt checks')
            report['build_nodes']=dict(Counter(row['status'] for row in results))
            if crash_after_build:os._exit(86)
        report['checks']=verify_complete(output,data,previous,impact)
        if before_db!=sha256(previous/'pilot.duckdb') or before_marker!=sha256(previous/'result.json'):raise ValueError('previous snapshot changed')
        previous_proof(previous)
        if code!=project_hashes(snapshot) or code!=project_hashes(project):raise ValueError('executed project changed')
        report.update(completed=True,previous_unchanged=True,untouched_periods_match=True,
            artifacts={**{name:sha256(output/name) for name in ['pilot.duckdb','inputs.json','next-snapshot.json']},
                       **{file.relative_to(output).as_posix():sha256(file) for file in source_files.iterdir()},
                       **{'project/'+name:digest for name,digest in code.items()}},
            seconds=round(time.perf_counter()-start,3))
    except Exception as error:report['failure']=type(error).__name__+': '+str(error)
    save(output,report)
    return report
