import json
import os
import time
from datetime import datetime,timezone
from pathlib import Path

from flux.validation.cdf_probe import compare,expected,release_interrupted
from flux.validation.pinning import sha256

NOW=datetime(2026,10,22,tzinfo=timezone.utc)


def load_probe(spark,path):
    from pyspark.sql import functions as F
    from flux.bronze.schema import COLUMNS

    return spark.read.parquet(str(path)).select(*[F.col(c.name).cast(c.type).alias(c.name) for c in COLUMNS])


def source_hashes():
    from flux.silver import build,dedup,monthly_listens,quality
    from flux.validation import cdf_probe

    modules=[build,dedup,monthly_listens,quality,cdf_probe]
    paths=[Path(m.__file__) for m in modules]+[Path(__file__)]
    return {p.name:sha256(p) for p in paths}


def save(root,result):
    path=Path(root)/'progress.tmp'
    path.write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    path.replace(Path(root)/'result.json')


def verify_inputs(root):
    root=Path(root);info=json.loads((root/'inputs.json').read_text(encoding='utf-8'))
    if info['prepared'] is not True or info['production_applied'] is not False:raise ValueError('not a prepared isolated experiment')
    for name,digest in info['artifacts'].items():
        path=(root/name).resolve()
        if root.resolve() not in path.parents or sha256(path)!=digest:raise ValueError('input artifact changed')
    return info


def versions(spark,root):
    from delta.tables import DeltaTable
    from flux.silver import monthly_listens as monthly

    return {name:DeltaTable.forPath(spark,str(root/name)).history(1).first()['version']
            for name in [monthly.TABLE,monthly.METADATA,monthly.QUARANTINE]}


def measure(spark,source,root,name,*,elapsed=0,previous=None):
    from delta.tables import DeltaTable
    from flux.silver import monthly_listens as monthly

    table=DeltaTable.forPath(spark,str(source));end=table.history(1).first()['version'];identifier=table.detail().first()['id']
    pinned=monthly.pin_snapshot(spark,root)
    states={n:monthly.checkpoint(spark,root/n) for n in pinned}
    if not all(s==monthly.state_for(identifier,end) for s in states.values()):raise ValueError('checkpoint/source mismatch')
    read=lambda path,version:spark.read.format('delta').option('versionAsOf',version).load(str(path))
    raw=read(source,end);records=[r.asDict() for r in raw.collect()]
    counts=[r.asDict() for r in read(root/monthly.TABLE,pinned[monthly.TABLE]).collect()]
    rejected=[(r['_source_dump'],r['_record_id'],r['reason'],r['raw_json'])
              for r in read(root/monthly.QUARANTINE,pinned[monthly.QUARANTINE]).collect()]
    metadata=[(r['recording_msid'],r['track_name'],r['artist_name'],r['recording_mbid'],
               tuple(r['artist_mbids']) if r['artist_mbids'] else None,r['mbid_source'])
              for r in read(root/monthly.METADATA,pinned[monthly.METADATA]).collect()]
    result=compare(expected(records),counts,rejected,metadata)
    full=monthly.aggregate(monthly.winners(raw,built_at=NOW))
    actual={(r['user_name'],r['recording_msid'],r['listened_month']):r['play_count'] for r in counts}
    rebuilt={(r.user_name,r.recording_msid,r.listened_month):r.play_count for r in full.collect()}
    if actual!=rebuilt:raise ValueError('pinned full Spark rebuild mismatch')
    feed={}
    if previous is not None and previous<end:
        changes=monthly.changes(spark,str(source),previous,end)
        try:feed={r['_change_type']:r['count'] for r in changes.groupBy('_change_type').count().collect()}
        finally:changes.unpersist()
    if table.history(1).first()['version']!=end:raise ValueError('source advanced during comparison')
    result.update(scenario=name,source_version=end,source_id=identifier,silver_versions=pinned,
                  reference_source_rows=len(records),cdf_rows_by_type=feed,refresh_seconds=round(elapsed,3),
                  full_spark_mismatches=0)
    print(json.dumps(result),flush=True)
    return result


def _refresh(spark,source,target,report,root,name,*,previous=None):
    from flux.silver import monthly_listens as monthly

    begin=time.perf_counter();monthly.refresh(spark,str(source),target,built_at=NOW)
    report['scenarios'].append(measure(spark,source,target,name,elapsed=time.perf_counter()-begin,previous=previous))
    save(root,report)


def partial_read_rejected(spark,target):
    from flux.silver import monthly_listens as monthly

    if monthly.ready(spark,target):raise ValueError('expected partial checkpoint')
    try:monthly.pin_snapshot(spark,target)
    except FileExistsError:return 'active_writer'
    except RuntimeError:return 'incomplete_checkpoint'
    raise ValueError('partial snapshot was readable')


def _synthetic(spark,number,stamp,*,dump='__cdf_a',played=None,user='__flux_cdf_probe',msid='probe-song',title='Probe'):
    from pyspark.sql import functions as F

    doc={'user_name':user,'timestamp':stamp,'recording_msid':msid,'track_metadata':{'track_name':title,'artist_name':'Probe Artist',
         'additional_info':{'ms_played':played}}}
    return spark.createDataFrame([(NOW,dump,'synthetic',number,stamp,user,json.dumps(doc),NOW.date())],
        '_ingested_at timestamp,_source_dump string,_source_file string,_record_id long,listened_at long,user_name string,raw_json string,ingested_date date')


def start(spark,root):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F
    from flux.bronze import ingest
    from flux.silver import monthly_listens as monthly

    root=Path(root);info=verify_inputs(root);source=root/'bronze';target=root/'silver'
    if source.exists() or target.exists() or (root/'result.json').exists():raise ValueError('new experiment required')
    report={'completed':False,'production_applied':False,'scope':info['scope'],'scenarios':[],
            'guards':{},'input_manifest_sha256':sha256(root/'inputs.json'),'retention_days':30,
            'executed_source_sha256':source_hashes()}
    save(root,report)
    base=load_probe(spark,root/'inputs/base.parquet')
    base.write.format('delta').partitionBy('ingested_date').save(str(source))
    table=DeltaTable.forPath(spark,str(source))
    _refresh(spark,source,target,report,root,'initial_without_cdf')
    spark.sql(f"ALTER TABLE delta.`{source}` SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true', 'delta.logRetentionDuration'='interval 30 days', 'delta.deletedFileRetentionDuration'='interval 30 days')")
    report['cdf_enabled_version']=table.history(1).first()['version']
    report['source_properties']=dict(table.detail().first()['properties'])
    _refresh(spark,source,target,report,root,'enable_cdf_metadata_only',previous=0)
    for i,entry in enumerate(info['archives']):
        for replay in range(2):
            before=table.history(1).first()['version']
            staged=ingest.build_dataframe(spark,str(root/entry['sample']),source_dump=entry['dump'],source_file=entry['sample'],ingested_at=NOW)
            valid,rejected=ingest.split(staged)
            if rejected.count() or not valid.count():raise ValueError('real sample must contain Bronze records')
            ingest.write(valid,str(source),source_dump=entry['dump'])
            _refresh(spark,source,target,report,root,f'real_dump_{i+1}'+('_replay' if replay else ''),previous=before)
            if replay:
                prior,after=report['scenarios'][-2:]
                if prior['plays']!=after['plays'] or prior['monthly_rows']!=after['monthly_rows']:raise ValueError('replay changed totals')
    unchanged=versions(spark,target)
    _refresh(spark,source,target,report,root,'no_new_version')
    if unchanged!=versions(spark,target):raise ValueError('unchanged source committed again')
    rows=[_synthetic(spark,1,1769903999),_synthetic(spark,2,1769904000),
          _synthetic(spark,3,1769904001,played=29999),_synthetic(spark,4,1769904002),
          _synthetic(spark,5,1769903999,dump='__cdf_b',title='Alternative')]
    frame=rows[0]
    for row in rows[1:]:frame=frame.unionByName(row)
    before=table.history(1).first()['version'];frame.write.format('delta').mode('append').save(str(source))
    _refresh(spark,source,target,report,root,'duplicate_arrival_and_boundary',previous=before)
    before=table.history(1).first()['version']
    moved=_synthetic(spark,2,1735689600,user='__flux_cdf_moved',msid='moved-song').first().asDict()
    table.update("_source_dump='__cdf_a' AND _record_id=2",{key:F.lit(moved[key]) for key in ['raw_json','user_name','listened_at']})
    _refresh(spark,source,target,report,root,'update_user_msid_month_year',previous=before)
    feed=report['scenarios'][-1]['cdf_rows_by_type']
    if feed.get('update_preimage')!=1 or feed.get('update_postimage')!=1:raise ValueError('real update images required')
    before=table.history(1).first()['version'];table.delete("_source_dump='__cdf_b'")
    _refresh(spark,source,target,report,root,'delete_winner_restore_loser',previous=before)
    before=table.history(1).first()['version']
    fixed=_synthetic(spark,3,1769904001,played=30000).first()['raw_json']
    spoiled=_synthetic(spark,4,1769904002,played=29999).first()['raw_json']
    table.update("_source_dump='__cdf_a' AND _record_id IN (3,4)",{'raw_json':F.when(F.col('_record_id')==3,F.lit(fixed)).otherwise(F.lit(spoiled))})
    _refresh(spark,source,target,report,root,'quality_valid_invalid_swap',previous=before)
    before=table.history(1).first()['version'];table.delete("_source_dump='__cdf_a'")
    _refresh(spark,source,target,report,root,'delete_last_keys_and_quarantine',previous=before)
    _guards(spark,root,source,target,report)
    before=table.history(1).first()['version']
    _synthetic(spark,90,1769904009,dump='__cdf_recovery').write.format('delta').mode('append').save(str(source))
    def stop_after_counts(*args,**kwargs):
        report['guards']['active_writer_snapshot_rejected']=partial_read_rejected(spark,target)=='active_writer'
        marker={'pid':os.getpid(),'expected_exit_code':86,'input_sha256':report['input_manifest_sha256'],
                'before_version':before,'after_version':table.history(1).first()['version'],
                'silver_versions':versions(spark,target),'counts_committed':True}
        (root/'interrupted.json').write_text(json.dumps(marker,indent=2)+'\n',encoding='utf-8')
        peak=Path('/sys/fs/cgroup/memory.peak')
        if peak.exists():report['start_container_peak_memory_bytes']=int(peak.read_text())
        if source_hashes()!=report['executed_source_sha256']:raise ValueError('executed source changed')
        report['awaiting_recovery']=True;save(root,report)
        print('EXPECTED_PROCESS_EXIT_86_AFTER_COUNTS_COMMIT',flush=True)
        os._exit(86)
    monthly.advance_metadata=stop_after_counts
    monthly.refresh(spark,str(source),target,built_at=NOW)
    raise ValueError('interruption was not reached')


def _guards(spark,root,source,target,report):
    from delta.tables import DeltaTable
    from flux.silver import monthly_listens as monthly

    original=monthly.checkpoint(spark,target/monthly.TABLE)
    def blocked(name,action):
        try:action()
        except monthly.FullRebuildNeeded:report['guards'][name]=True
        else:raise ValueError('expected rebuild guard: '+name)
    invalid={**original,'minimum_ms_played':10000}
    monthly.touch(spark,target/monthly.TABLE,invalid)
    blocked('changed_policy_rejected',lambda:monthly.refresh(spark,str(source),target,built_at=NOW))
    monthly.touch(spark,target/monthly.TABLE,original)
    monthly.touch(spark,target/monthly.TABLE,monthly.state_for(original['source_id'],original['version']+100))
    blocked('future_checkpoint_rejected',lambda:monthly.refresh(spark,str(source),target,built_at=NOW))
    monthly.touch(spark,target/monthly.TABLE,original)
    other=root/'other-bronze'
    spark.read.format('delta').load(str(source)).write.format('delta').save(str(other))
    blocked('replaced_source_rejected',lambda:monthly.refresh(spark,str(other),target,built_at=NOW))
    missing=root/'missing-feed';derived=root/'missing-feed-silver'
    _synthetic(spark,1,1769904000).write.format('delta').save(str(missing))
    monthly.refresh(spark,str(missing),derived,built_at=NOW)
    prior=versions(spark,derived)
    _synthetic(spark,2,1769904001).write.format('delta').mode('append').save(str(missing))
    blocked('missing_cdf_rejected',lambda:monthly.refresh(spark,str(missing),derived,built_at=NOW))
    if prior!=versions(spark,derived):raise ValueError('missing CDF modified target')
    report['guards']['missing_cdf_target_unchanged']=True
    save(root,report)


def recover(spark,root,*,confirmed_exit_code):
    from flux.silver import monthly_listens as monthly

    root=Path(root);verify_inputs(root);report=json.loads((root/'result.json').read_text(encoding='utf-8'))
    if report['completed'] or not report.get('awaiting_recovery'):raise ValueError('not an interrupted experiment')
    if source_hashes()!=report['executed_source_sha256']:raise ValueError('recovery source differs')
    marker=release_interrupted(root,confirmed_exit_code=confirmed_exit_code,input_sha256=sha256(root/'inputs.json'))
    source=root/'bronze';target=root/'silver'
    if versions(spark,target)!=marker['silver_versions']:raise ValueError('interrupted tables changed')
    report['guards']['partial_snapshot_rejected']=partial_read_rejected(spark,target)=='incomplete_checkpoint'
    _refresh(spark,source,target,report,root,'recover_after_process_exit',previous=marker['before_version'])
    after=versions(spark,target)
    if after[monthly.TABLE]!=marker['silver_versions'][monthly.TABLE]:raise ValueError('counts were added twice on recovery')
    _refresh(spark,source,target,report,root,'recovered_no_new_version')
    if after!=versions(spark,target):raise ValueError('recovered replay wrote changes')
    verify_inputs(root)
    report.update(completed=True,awaiting_recovery=False,inputs_unchanged=True,production_applied=False,
                  forced_exit_code=confirmed_exit_code,counts_reapplied_on_recovery=False,
                  runtime={'spark':spark.version,'python':os.sys.version.split()[0]})
    peak=Path('/sys/fs/cgroup/memory.peak')
    if peak.exists():report['recovery_container_peak_memory_bytes']=int(peak.read_text())
    save(root,report)
    return report
