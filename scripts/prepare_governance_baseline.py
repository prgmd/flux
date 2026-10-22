import argparse
import importlib.metadata
import json
import platform
import shutil
import subprocess
import time
from pathlib import Path

import duckdb

from flux.gold.monthly_marts import literal
from flux.validation.baseline import copy_selection,freeze_sample,replay_sample
from flux.validation.dumps import dump_header,summarize_windows
from flux.validation.pinning import check_pin,pin_delta,sha256,verify_bundle


def write(path,value):
    Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,default=str,allow_nan=False)+'\n',encoding='utf-8')


def run(bronze,silver,audit,gold,official,redirects,review,dumps,output):
    bronze,silver,audit,gold,official,redirects,review,dumps,output=map(lambda p:Path(p).resolve(),
        (bronze,silver,audit,gold,official,redirects,review,dumps,output))
    if output.exists():raise FileExistsError(output)
    for source in (bronze,silver,audit,gold,official,redirects,review,dumps):
        if output==source or output in source.parents or source in output.parents:raise ValueError('overlapping baseline paths')
    if (silver/'.writer.lock').exists():raise ValueError('Silver writer active')
    started=time.monotonic()
    pins={'bronze':pin_delta(bronze)}
    for name in ['monthly_user_recordings','recording_metadata','invalid_listens']:pins[name]=pin_delta(silver/name)
    states=[pins[name]['monthly_state'] for name in ['monthly_user_recordings','recording_metadata','invalid_listens']]
    if not states[0] or not all(state==states[0] for state in states):raise ValueError('inconsistent Silver processing state')
    if states[0]['source_id']!=pins['bronze']['id'] or states[0]['version']!=pins['bronze']['version']:
        raise ValueError('Silver does not correspond to pinned Bronze')
    info=json.loads((gold/'result.json').read_text(encoding='utf-8'))
    audit_info=json.loads((audit/'result.json').read_text(encoding='utf-8'))
    fingerprint={name:pins[name]['log_sha256'] for name in ['recording_metadata','monthly_user_recordings']}
    for value in [info,audit_info]:
        if not value['completed'] or value['source_after']!=fingerprint or not value['source_unchanged']:
            raise ValueError('stale or unfinished Gold/audit')
    if info['audit_manifest']['result.json']!=sha256(audit/'result.json'):raise ValueError('Gold audit mismatch')
    source_files=[gold/'result.json',audit/'result.json',dumps,silver/'migration.json',silver/'verification.json',
                  official/'result.json',redirects/'result.json',review/'result.json',review/'production_export.json',
                  review/'facts.json',review/'candidates.jsonl',review/'decisions.jsonl']
    source_files.extend(audit/(name+'.parquet') for name in ['reviewed','proposed_credits','resolution'])
    source_files.extend(gold/(name+'.parquet') for name in ['song_dimension','song_labels','song_credits'])
    source_files.extend(gold/period/(name+'.parquet') for period,tables in info['counts'].items() for name in tables)
    source_files.extend(gold/name for name in ['sample-verification.json','attribution-verification.json'] if (gold/name).exists())
    print('Pinning source manifests and existing Gold bytes',flush=True)
    hashes={str(path):sha256(path) for path in source_files}
    references=json.loads((review/'result.json').read_text(encoding='utf-8'))
    if not references['completed'] or references['source_state']['silver_fingerprint']!=fingerprint:raise ValueError('stale review reference')
    approvals=json.loads((review/'production_export.json').read_text(encoding='utf-8'))
    sample=json.loads((review/'facts.json').read_text(encoding='utf-8'))
    seeds={row['original']['recording_msid'] for row in sample}
    with duckdb.connect() as c:
        c.execute('set threads=2');c.execute("set memory_limit='4GB'")
        c.read_parquet(str(gold/'song_labels.parquet')).create_view('all_labels')
        version_keys=[]
        for tag in ['live','remix','sped_up']:
            row=c.execute("select song_key from all_labels where regexp_matches(variant,?) order by song_key limit 1",['(^|[+])'+tag+'($|[+])']).fetchone()
            if not row:raise ValueError('version case missing: '+tag)
            version_keys.append(row[0])
        c.read_parquet(str(gold/'song_dimension.parquet')).create_view('all_dimension')
        seeds.update(r[0] for r in c.execute('select min(recording_msid) from all_dimension where song_key in(select unnest(?)) group by song_key',[version_keys]).fetchall())
        verification=json.loads((gold/'sample-verification.json').read_text(encoding='utf-8'))
        known_songs=[row['key'] for row in verification['checks'] if row['kind']=='song']
        seeds.update(r[0] for r in c.execute("select min(recording_msid) from all_dimension where recording_mbid in(select unnest(?)) and status='matched_candidate' group by song_key",[known_songs]).fetchall())
        monthly_paths=[row['path'] for row in pins['monthly_user_recordings']['files']]
        print('Copying closed song scope and comparing 14 small marts',flush=True)
        result=freeze_sample(c,monthly_paths,audit,gold,sorted(seeds),output)
        write(output/'selection.json',{'seed_msids':sorted(seeds),'protected_version_song_keys':version_keys,
              'prior_verified_song_mbids':sorted(set(known_songs)),'selection':'purposeful cases, all MSIDs per chosen song key and all their monthly rows'})
        chosen=[r[0] for r in c.execute('select recording_msid from selected_dimension').fetchall()]
        c.read_parquet([r['path'] for r in pins['recording_metadata']['files']]).create_view('source_metadata')
        copy_selection(c,'select * from source_metadata where recording_msid in(select unnest(?))',[chosen],output/'inputs/recording_metadata.parquet')
        c.execute('copy (select * from read_parquet('+literal(pins['bronze']['files'][0]['path'])+',hive_partitioning=true) limit 256) to ? (format parquet,compression zstd)',[str(output/'inputs/bronze_probe.parquet')])
        c.execute('copy (select * from read_parquet('+literal(pins['invalid_listens']['files'][0]['path'])+') limit 64) to ? (format parquet,compression zstd)',[str(output/'inputs/quarantine_probe.parquet')])
        features={
            'repeated_monthly_rows':c.execute('select count(*) from monthly where play_count>1').fetchone()[0],
            'multi_msid_song_keys':c.execute('select count(*) from song_labels where source_msids>1').fetchone()[0],
            'unresolved_songs':c.execute("select count(*) from song_labels where identity_status<>'matched_candidate'").fetchone()[0],
            'multi_credit_songs':c.execute('select count(*) from (select song_key from song_credits group by song_key having count(*)>1)').fetchone()[0],
            'account_song_across_months':c.execute('select count(*) from (select user_name,d.song_key from monthly m join song_dimension d using(recording_msid) group by user_name,d.song_key having count(distinct listened_month)>1)').fetchone()[0],
            'null_observed_mbid_metadata':c.execute('select count(*) from read_parquet(?) where recording_mbid is null',[str(output/'inputs/recording_metadata.parquet')]).fetchone()[0],
            'conflicting_msid_metadata':c.execute('select count(*) from (select recording_msid from read_parquet(?) group by recording_msid having count(distinct recording_mbid)>1)',[str(output/'inputs/recording_metadata.parquet')]).fetchone()[0],
            'late_arrival_probe_rows':c.execute('select count(*) from read_parquet(?) where listened_at<epoch(_ingested_at)-2592000',[str(output/'inputs/bronze_probe.parquet')]).fetchone()[0],
            'variant_labels':[r[0] for r in c.execute('select distinct variant from song_labels order by variant').fetchall()]}
        for name,value in features.items():
            if isinstance(value,int) and value<1:raise ValueError('representative case absent: '+name)
        write(output/'features.json',features)
    print('Checking locally retained dump windows',flush=True)
    inventory=json.loads(dumps.read_text(encoding='utf-8'))
    records=[dump_header(entry) for entry in inventory['entries'] if entry['file'].endswith('.tar.zst')]
    write(output/'dump_inventory.json',{'records':records,'summary':summarize_windows(records),
          'new_downloads':False,'catalog_checked_online':False})
    versions={}
    for package in ['duckdb','pyspark','delta-spark','dbt-core','dbt','dbt-duckdb','great-expectations','soda-core']:
        try:versions[package]=importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:versions[package]=None
    write(output/'compatibility.json',{'python':platform.python_version(),'installed_distributions':versions,
          'dbt_gx_runtime_verified':False,'next_checks':['isolated environment','read pinned inputs','run models and tests','generate docs and check catalog artifacts'],
          'listener_key':'user_name','rank':'rank by listeners; tied ranks and name-order display',
          'old_spark_period_marts_comparable':False})
    documents=output/'source_manifests';documents.mkdir()
    for root,name in [(gold,'gold'),(audit,'attribution'),(official,'official'),(redirects,'redirects'),(review,'review')]:
        shutil.copy2(root/'result.json',documents/(name+'.json'))
    shutil.copy2(review/'production_export.json',documents/'r9_production_export.json')
    code_files=['src/flux/gold/monthly_marts.py','src/flux/validation/baseline.py','src/flux/validation/oracle.py',
                'src/flux/validation/pinning.py','src/flux/validation/dumps.py','scripts/prepare_governance_baseline.py']
    for name in code_files:
        target=output/'code'/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(name,target)
    write(output/'source_context.json',{'tables':pins,'source_file_sha256':hashes,'source_gold':str(gold),
          'audit':str(audit),'gold_policies':{k:v for k,v in info.items() if 'policy' in k or k in ['timezone','event_deduplication']},
          'r9_reference_only':True,'r9_approved_candidates':approvals['approved_candidates'],
          'code_head':subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),
          'code_sha256':{name:sha256(name) for name in code_files},'code_frozen_in_bundle':True,
          'large_source_data_guard':'Delta JSON hashes, active paths, sizes and mtime; copied baseline data gets SHA-256'})
    replay=replay_sample(output)
    for pin in pins.values():check_pin(pin)
    if any(sha256(path)!=value for path,value in hashes.items()):raise ValueError('reference source changed')
    for row in records:
        stat=Path(row['path']).stat()
        if (stat.st_size,stat.st_mtime_ns)!=(row['bytes'],row['mtime_ns']):raise ValueError('dump changed')
    result.update(completed=True,source_unchanged=True,baseline_ready=True,
                  p0_collection_ready=False,tool_compatibility_pending=True,
                  features=features,replay=replay,seconds=round(time.monotonic()-started,2),
                  artifacts={str(path.relative_to(output)).replace('\\','/'):sha256(path) for path in sorted(output.rglob('*')) if path.is_file()})
    write(output/'result.json',result)
    verify_bundle(output)
    print(json.dumps({key:result[key] for key in ['monthly_rows','plays','source_msids','song_keys','oracle_tables_checked','full_song_reference_rows','seconds']},ensure_ascii=False),flush=True)
    return result


if __name__=='__main__':
    parser=argparse.ArgumentParser()
    for name in ['bronze','silver','audit','gold','official','redirects','review','dumps','output']:parser.add_argument('--'+name,type=Path,required=True)
    args=parser.parse_args();run(**vars(args))
