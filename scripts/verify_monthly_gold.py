import argparse
import hashlib
import json
import math
from collections import defaultdict
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.gold.monthly_marts import literal


def verify_samples(silver, audit, gold, song_mbids, artist_mbids):
    manifest=json.loads((gold/'result.json').read_text(encoding='utf-8'))
    report=gold/'sample-verification.json'
    report.write_text(json.dumps({'completed':False}),encoding='utf-8')
    def fingerprint():
        return {name:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(silver/name))).hexdigest()
                for name in ('recording_metadata','monthly_user_recordings')}
    before=fingerprint()
    if not manifest['completed'] or before!=manifest['source_after']:
        raise ValueError('Gold source mismatch')
    if hashlib.sha256((audit/'result.json').read_bytes()).hexdigest()!=manifest['audit_manifest']['result.json']:
        raise ValueError('audit mismatch')
    if not song_mbids and not artist_mbids:
        raise ValueError('no samples')
    c=duckdb.connect()
    c.execute('set threads=2')
    c.execute("set memory_limit='4GB'")
    c.read_parquet(deltalog.active_paths(silver/'monthly_user_recordings'),hive_partitioning=True).create_view('monthly')
    for name in ('reviewed','proposed_credits'):
        c.read_parquet(str(audit/(name+'.parquet'))).create_view(name)
    checks=[]
    for kind,key in [('song',x) for x in song_mbids]+[('artist',x) for x in artist_mbids]:
        if kind=='song':
            c.execute("create or replace temp table selected as select recording_msid from reviewed where status='matched_candidate' and recording_mbid=?",[key])
        else:
            c.execute('create or replace temp table selected as select distinct recording_msid from proposed_credits where artist_mbid=?',[key])
        groups={p:defaultdict(lambda:defaultdict(int)) for p in ('month','year')}
        cur=c.execute('select user_name,listened_month,play_count from monthly semi join selected using(recording_msid)')
        while rows:=cur.fetchmany(100000):
            for user,month,count in rows:
                groups['month'][str(month)[:7]+'-01'][user]+=count
                groups['year'][str(month)[:4]+'-01-01'][user]+=count
        if not groups['month']:
            raise ValueError('empty sample: '+key)
        for period,expected in groups.items():
            table='song_metrics' if kind=='song' else 'artist_metrics'
            field='song_key' if kind=='song' else 'artist_mbid'
            value='mbid:'+key if kind=='song' else key
            actual=c.execute(f"select period_start,listens,listeners,top_user_listens,concentration_hhi from read_parquet({literal(gold/period/(table+'.parquet'))}) where {field}=?",[value]).fetchall()
            if len(actual)!=len(expected):
                raise ValueError('sample period mismatch: '+key)
            for date,total,n,top,hhi in actual:
                values=list(expected[str(date)].values())
                if not values or (total,n,top)!=(sum(values),len(values),max(values)):
                    raise ValueError('sample metric mismatch: '+key)
                if not math.isclose(hhi,sum(v*v for v in values)/sum(values)**2,rel_tol=1e-12):
                    raise ValueError('sample concentration mismatch: '+key)
            checks.append({'kind':kind,'key':key,'period':period,'periods_checked':len(actual)})
    if before!=fingerprint():
        raise ValueError('source changed')
    c.close()
    result={'completed':True,'checks':checks,'periods_checked':sum(x['periods_checked'] for x in checks)}
    report.write_text(json.dumps(result,indent=2),encoding='utf-8')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for name in ('silver','audit','gold'):
        p.add_argument('--'+name,type=Path,required=True)
    p.add_argument('--song-mbid',action='append',default=[])
    p.add_argument('--artist-mbid',action='append',default=[])
    a=p.parse_args()
    print(json.dumps(verify_samples(a.silver,a.audit,a.gold,a.song_mbid,a.artist_mbid)),flush=True)
