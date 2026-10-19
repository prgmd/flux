import argparse
import hashlib
import json
import time
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.gold.monthly_marts import build_dimension, build_period, literal


EXPORTS = {
    'song_metrics': 'period_start,listener_rank,song_name,song_key',
    'artist_metrics': 'period_start,listener_rank,artist_name,artist_mbid',
    'artist_song_metrics': 'period_start,artist_name,artist_mbid,artist_song_rank,song_name,song_key',
    'coverage': 'period_start',
    'variant_metrics': 'period_start,variant',
    'listener_metrics': 'period_start,listener_key',
    'identity_coverage': 'period_start,identity_status',
}


def fingerprint(silver):
    return {name: hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(silver/name))).hexdigest()
            for name in ('recording_metadata','monthly_user_recordings')}


def run(silver, audit, output):
    silver,audit,output=(p.resolve() for p in (silver,audit,output))
    if any(p==output or p in output.parents or output in p.parents for p in (silver,audit)):
        raise ValueError('overlapping output')
    info=json.loads((audit/'result.json').read_text(encoding='utf-8'))
    before=fingerprint(silver)
    if not info['completed'] or not info['source_unchanged'] or before!=info['source_after']:
        raise ValueError('audit does not match Silver')
    output.mkdir(parents=True,exist_ok=False)
    start=time.monotonic()
    c=duckdb.connect(str(output/'gold.duckdb'))
    c.execute('set threads=2')
    c.execute("set memory_limit='8GB'")
    c.execute("set max_temp_directory_size='100GB'")
    c.execute('set preserve_insertion_order=false')
    c.read_parquet(deltalog.active_paths(silver/'monthly_user_recordings'),hive_partitioning=True).create_view('monthly')
    for name in ('reviewed','proposed_credits'):
        c.read_parquet(str(audit/(name+'.parquet'))).create_view(name)
    audit_hashes={name:hashlib.sha256((audit/name).read_bytes()).hexdigest() for name in ('result.json',)}
    build_dimension(c)
    if c.execute('select count(*)-count(distinct recording_msid) from song_dimension').fetchone()[0]:
        raise ValueError('duplicate MSID')
    missing=c.execute('select count(*) from monthly anti join song_dimension using(recording_msid)').fetchone()[0]
    if missing:
        raise ValueError('missing MSID')
    print('dimension ready',flush=True)
    counts={}
    for period in ('month','year'):
        print('building '+period,flush=True)
        build_period(c,period)
        counts[period]={}
        directory=output/period
        directory.mkdir()
        for name,order in EXPORTS.items():
            c.execute(f'copy (select * from {name} order by {order}) to {literal(directory/(name+".parquet"))} (format parquet, compression zstd)')
            count=c.execute(f'select count(*) from {name}').fetchone()[0]
            exported=c.execute(f'select count(*) from read_parquet({literal(directory/(name+".parquet"))})').fetchone()[0]
            if count!=exported:
                raise ValueError('export mismatch')
            counts[period][name]=count
        print(json.dumps({period:counts[period]}),flush=True)
    for name in ('song_dimension','song_labels','song_credits'):
        c.execute(f'copy {name} to {literal(output/(name+".parquet"))} (format parquet,compression zstd)')
    after=fingerprint(silver)
    if before!=after or audit_hashes['result.json']!=hashlib.sha256((audit/'result.json').read_bytes()).hexdigest():
        raise ValueError('input changed')
    total=c.execute('select sum(play_count) from monthly').fetchone()[0]
    for name in ('user_songs','user_artists','user_variants'):
        c.execute('drop table '+name)
    c.execute('checkpoint')
    c.close()
    result={'completed':True,'source_before':before,'source_after':after,'source_unchanged':True,
            'silver':str(silver),'audit':str(audit),'audit_manifest':audit_hashes,
            'counts':counts,'listens':total,'seconds':round(time.monotonic()-start,2),
            'format':'parquet snapshots and DuckDB','periods':['month','year'],'timezone':'UTC',
            'identity_policy':'matched official MBID only; unresolved MSIDs retained separately',
            'variant_policy':'variant-label-v1; modified versions never stripped to base title',
            'credit_policy':info.get('credit_policy','official matched candidates only, full credit to each participant'),
            'rank_policy':'rank by listeners only; display name then stable key',
            'event_deduplication':'no cross-MSID event subtraction', 'production_promoted':False}
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--silver',type=Path,required=True)
    p.add_argument('--audit',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    run(a.silver,a.audit,a.output)
