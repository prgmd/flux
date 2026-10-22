import argparse
import hashlib
import json
import time
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.review.pilot import review_sample,write_json


def digest(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk:=stream.read(8*1024*1024):value.update(chunk)
    return value.hexdigest()


def fingerprint(silver):
    return {name:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(silver/name))).hexdigest()
            for name in ('recording_metadata','monthly_user_recordings')}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--sample',type=Path,required=True)
    parser.add_argument('--prepared',type=Path,required=True)
    parser.add_argument('--silver',type=Path,required=True)
    parser.add_argument('--snapshot',type=Path,required=True)
    parser.add_argument('--redirects',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    for path in (args.sample,args.prepared,args.silver,args.snapshot,args.redirects):
        a,b=args.output.resolve(),path.resolve()
        if a==b or a in b.parents or b in a.parents:raise ValueError('overlapping input/output')
    if args.output.exists():raise FileExistsError(args.output)
    started=time.monotonic()
    sample=json.loads((args.sample/'result.json').read_text(encoding='utf-8'))
    preview=json.loads((args.sample/'rows.json').read_text(encoding='utf-8'))
    before=fingerprint(args.silver)
    if not sample['completed'] or not sample['source_unchanged'] or before!=sample['source_before']:
        raise ValueError('sample source is not current')
    descriptors={}
    pinned={path:digest(path) for path in (args.sample/'result.json',args.sample/'rows.json',args.snapshot/'result.json',args.redirects/'result.json')}
    stats={}
    for root,names in ((args.snapshot,('recording','artist','artist_credit_name')),(args.redirects,('recording_gid_redirect',))):
        manifest=json.loads((root/'result.json').read_text(encoding='utf-8'))
        if not manifest['completed']:raise ValueError('unfinished official snapshot')
        descriptors[str(root.resolve())]=manifest
        for name in names:
            path=(root/name).resolve();info=manifest['tables'][name]
            print('Verifying official snapshot: '+name,flush=True)
            if path.stat().st_size!=info['bytes'] or digest(path)!=info['sha256']:
                raise ValueError('official snapshot hash mismatch: '+name)
            stats[path]=(path.stat().st_size,path.stat().st_mtime_ns)
    if len({r['source_release'] for r in descriptors.values()})!=1:raise ValueError('different official releases')
    snapshot_id='musicbrainz:'+hashlib.sha256(json.dumps(descriptors,sort_keys=True).encode()).hexdigest()
    state={'silver_fingerprint':before,'official_snapshot':snapshot_id,
           'sample_sha256':pinned[args.sample/'rows.json'],'sample_result_sha256':pinned[args.sample/'result.json']}
    with duckdb.connect(str(args.prepared/'experiment.duckdb'),read_only=True) as c:
        c.execute("set threads=2");c.execute("set memory_limit='4GB'")
        for view,root,filename in (('raw_recording',args.snapshot,'recording'),('raw_artist',args.snapshot,'artist'),
                ('raw_artist_credit_name',args.snapshot,'artist_credit_name'),('raw_recording_gid_redirect',args.redirects,'recording_gid_redirect')):
            sql=c.execute('select sql from duckdb_views() where view_name=?',[view]).fetchone()[0]
            if str((root/filename).resolve()) not in sql:raise ValueError('prepared official source mismatch: '+view)
        paths=deltalog.active_paths(args.silver/'recording_metadata')
        quoted=','.join("'"+path.replace("'","''")+"'" for path in paths)
        c.execute('create temp view metadata as select * from read_parquet(['+quoted+'],hive_partitioning=true)')
        print('Reviewing sample and validating monthly sandbox',flush=True)
        result=review_sample(c,sample,preview,state,deltalog.active_paths(args.silver/'monthly_user_recordings'),args.output)
    after=fingerprint(args.silver)
    unchanged=(before==after and all(digest(path)==value for path,value in pinned.items())
               and all((path.stat().st_size,path.stat().st_mtime_ns)==value for path,value in stats.items()))
    result.update(completed=unchanged,source_after=after,source_unchanged=unchanged,seconds=round(time.monotonic()-started,2),
                  official_snapshot_verified_tables=[str(path) for path in stats])
    write_json(args.output/'result.json',result)
    if not unchanged:raise ValueError('source changed during review')
    print(json.dumps({key:result[key] for key in ('sample_rows','candidate_pairs','latest_decisions','human_decisions','monthly_validation','seconds')},ensure_ascii=False),flush=True)


if __name__=='__main__':main()
