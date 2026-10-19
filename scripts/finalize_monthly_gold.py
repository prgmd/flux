import argparse
import hashlib
import json
import shutil
import time
from pathlib import Path

import duckdb

from flux.gold.monthly_marts import literal


COUNTS={'listens','listeners','top_user_listens','period_listens','period_users','linked_listens',
        'linked_users','song_count','listener_rank','listen_rank','artist_song_rank','source_msids'}


def finalize(source, target):
    source,target=source.resolve(),target.resolve()
    if source==target or source in target.parents or target in source.parents:
        raise ValueError('overlapping snapshot')
    raw=(source/'result.json').read_bytes()
    manifest=json.loads(raw)
    if not manifest['completed']:
        raise ValueError('incomplete source')
    target.mkdir(parents=True,exist_ok=False)
    start=time.monotonic()
    c=duckdb.connect(str(target/'gold.duckdb'))
    c.execute('set threads=2');c.execute("set memory_limit='4GB'")
    c.execute('set preserve_insertion_order=true')
    checked=[]
    for period,tables in manifest['counts'].items():
        (target/period).mkdir()
        for name,count in tables.items():
            src=source/period/(name+'.parquet');dst=target/period/(name+'.parquet')
            c.read_parquet(str(src)).create_view('input_table',replace=True)
            schema=c.execute('describe input_table').fetchall()
            fields=[row[0] for row in schema if row[0] in COUNTS]
            for column,dtype,*_ in schema:
                if column in COUNTS and dtype in ('DOUBLE','FLOAT'):
                    invalid=c.execute(f'select count(*) from input_table where not isfinite({column}) or {column}<>trunc({column}) or abs({column})>9007199254740991').fetchone()[0]
                    if invalid:
                        raise ValueError('inexact count: '+column)
            replacements=','.join(f'cast({field} as bigint) as {field}' for field in fields)
            select='select *'+(' replace('+replacements+')' if fields else '')+' from input_table'
            c.execute(f'copy ({select}) to {literal(dst)} (format parquet, compression zstd)')
            c.read_parquet(str(dst)).create_view(period+'_'+name,replace=True)
            if c.execute(f'select count(*) from {period}_{name}').fetchone()[0]!=count:
                raise ValueError('row count mismatch')
            for field in fields:
                before=c.execute(f'select sum(cast({field} as hugeint)) from input_table').fetchone()[0]
                after=c.execute(f'select sum(cast({field} as hugeint)) from {period}_{name}').fetchone()[0]
                if before!=after:
                    raise ValueError('count changed: '+field)
            checked.append(period+'/'+name)
        print('finalized '+period,flush=True)
    c.execute('drop view input_table')
    for name in ('song_dimension','song_labels','song_credits'):
        shutil.copy2(source/(name+'.parquet'),target/(name+'.parquet'))
        c.read_parquet(str(target/(name+'.parquet'))).create_view(name)
    c.close()
    if raw!=(source/'result.json').read_bytes():
        raise ValueError('source manifest changed')
    manifest.update({'format':'typed Parquet snapshots with DuckDB views','finalized_from':str(source),
                     'source_manifest_sha256':hashlib.sha256(raw).hexdigest(),
                     'integer_counts_verified':True,'finalized_tables':checked,
                     'finalization_seconds':round(time.monotonic()-start,2)})
    (target/'result.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    return manifest


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--source',type=Path,required=True);p.add_argument('--output',type=Path,required=True)
    a=p.parse_args()
    print(json.dumps(finalize(a.source,a.output)),flush=True)
