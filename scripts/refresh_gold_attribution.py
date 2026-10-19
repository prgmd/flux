import argparse
import hashlib
import json
import shutil
import time
from pathlib import Path

import duckdb

from flux.common import deltalog
from flux.gold.monthly_marts import build_dimension, literal, metrics


def run(silver, audit, previous, output):
    silver,audit,previous,output=(p.resolve() for p in (silver,audit,previous,output))
    if any(p==output or p in output.parents or output in p.parents for p in (silver,audit,previous)):
        raise ValueError('overlapping output')
    def fingerprint():
        return {n:hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(silver/n))).hexdigest()
                for n in ('recording_metadata','monthly_user_recordings')}
    old=json.loads((previous/'result.json').read_text())
    info=json.loads((audit/'result.json').read_text())
    before=fingerprint()
    if not old['completed'] or not info['completed'] or before!=old['source_after'] or before!=info['source_after']:
        raise ValueError('inconsistent source')
    output.mkdir(parents=True,exist_ok=False)
    start=time.monotonic()
    c=duckdb.connect(str(output/'calculation.duckdb'))
    c.execute('set threads=2');c.execute("set memory_limit='6GB'")
    c.execute('set preserve_insertion_order=false')
    c.read_parquet(deltalog.active_paths(silver/'monthly_user_recordings'),hive_partitioning=True).create_view('monthly')
    for n in ('reviewed','proposed_credits'):
        c.read_parquet(str(audit/(n+'.parquet'))).create_view(n)
    build_dimension(c)
    for n in ('song_dimension','song_labels'):
        if c.execute(f"select count(*) from ((select * from {n} except select * from read_parquet({literal(previous/(n+'.parquet'))})) union all (select * from read_parquet({literal(previous/(n+'.parquet'))}) except select * from {n}))").fetchone()[0]:
            raise ValueError('song identity changed: '+n)
        shutil.copy2(previous/(n+'.parquet'),output/(n+'.parquet'))
    c.execute(f"copy song_credits to {literal(output/'song_credits.parquet')} (format parquet,compression zstd)")
    counts={}
    for period in ('month','year'):
        print('refreshing '+period,flush=True)
        directory=output/period;directory.mkdir()
        for path in (previous/period).glob('*.parquet'):
            if path.stem not in ('artist_metrics','artist_song_metrics','coverage'):
                shutil.copy2(path,directory/path.name)
        c.execute(f"""create or replace table user_artists as select
            date_trunc('{period}',cast(substr(cast(listened_month as varchar),1,7)||'-01' as date))::date period_start,
            user_name,artist_mbid,sum(play_count)::bigint plays
            from monthly join (select distinct recording_msid,artist_mbid from proposed_credits) using(recording_msid)
            group by period_start,user_name,artist_mbid""")
        metrics(c,'artist_base','user_artists',['artist_mbid'])
        c.execute("""create or replace table artist_metrics as select b.*,a.artist_name,
            rank() over(partition by period_start order by listeners desc) listener_rank,
            rank() over(partition by period_start order by listens desc) listen_rank
            from artist_base b join (select artist_mbid,min(artist_name) artist_name from song_credits group by artist_mbid) a using(artist_mbid)""")
        c.execute(f"""create or replace table artist_song_metrics as select s.*,a.artist_mbid,a.artist_name,
            s.listens::double/b.listens song_listen_share,s.listeners::double/b.listeners song_listener_reach,
            rank() over(partition by s.period_start,a.artist_mbid order by s.listeners desc) artist_song_rank
            from read_parquet({literal(directory/'song_metrics.parquet')}) s join song_credits a using(song_key)
            join artist_metrics b on s.period_start=b.period_start and a.artist_mbid=b.artist_mbid""")
        c.execute(f"""create or replace table coverage as select
            date_trunc('{period}',cast(substr(cast(listened_month as varchar),1,7)||'-01' as date))::date period_start,
            sum(play_count)::bigint period_listens,count(distinct user_name) period_users,
            coalesce(sum(play_count) filter(where recording_msid in(select recording_msid from proposed_credits)),0)::bigint linked_listens,
            count(distinct user_name) filter(where recording_msid in(select recording_msid from proposed_credits)) linked_users,
            linked_listens::double/period_listens artist_coverage from monthly group by period_start""")
        if c.execute('select sum(period_listens) from coverage').fetchone()[0]!=old['listens']:
            raise ValueError('total changed')
        if c.execute('select count(*) from artist_metrics where listeners>listens or top_user_share>1 or top_user_share<=0').fetchone()[0]:
            raise ValueError('invalid artist metrics')
        if c.execute('select count(*) from (select period_start,artist_mbid,sum(song_listen_share) s from artist_song_metrics group by all) where abs(s-1)>1e-9').fetchone()[0]:
            raise ValueError('artist song shares mismatch')
        orders={'artist_metrics':'period_start,listener_rank,artist_name,artist_mbid',
                'artist_song_metrics':'period_start,artist_name,artist_mbid,artist_song_rank,song_name,song_key',
                'coverage':'period_start'}
        for n,order in orders.items():
            c.execute(f'copy (select * from {n} order by {order}) to {literal(directory/(n+".parquet"))} (format parquet,compression zstd)')
        counts[period]={p.stem:c.execute(f'select count(*) from read_parquet({literal(p)})').fetchone()[0] for p in directory.glob('*.parquet')}
        print(json.dumps(counts[period]),flush=True)
    if before!=fingerprint():
        raise ValueError('Silver changed')
    c.close()
    with duckdb.connect(str(output/'gold.duckdb')) as views:
        for path in output.rglob('*.parquet'):
            name=(path.parent.name+'_' if path.parent!=output else '')+path.stem
            views.execute(f'create view {name} as select * from read_parquet({literal(path)})')
    result={**old,'audit':str(audit),'counts':counts,'seconds':round(time.monotonic()-start,2),
            'audit_manifest':{'result.json':hashlib.sha256((audit/'result.json').read_bytes()).hexdigest()},
            'credit_policy':info['credit_policy'],'attribution_policy':info['policy'],
            'previous_gold':str(previous),'unchanged_song_identity':True,
            'operation':'artist attribution refresh; other marts copied byte-for-byte'}
    for key in ('finalized_from','source_manifest_sha256','finalized_tables','finalization_seconds'):
        result.pop(key,None)
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    for n in ('silver','audit','previous','output'):
        p.add_argument('--'+n,type=Path,required=True)
    a=p.parse_args();run(a.silver,a.audit,a.previous,a.output)
