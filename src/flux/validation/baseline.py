from pathlib import Path

from flux.gold.monthly_marts import build_dimension,build_period,literal
from flux.validation.oracle import KEYS,compare_rows,python_marts
from flux.validation.pinning import check_pin,pin_delta,sha256,verify_bundle


def rows(c,table):
    result=c.execute('select * from '+table)
    names=[col[0] for col in result.description]
    return [dict(zip(names,row)) for row in result.fetchall()]


def copy_selection(c,query,parameters,path):
    c.execute('copy ('+query+') to '+literal(path)+' (format parquet,compression zstd)',parameters)


def replay_sample(root):
    import duckdb

    root=Path(root)
    with duckdb.connect() as c:
        c.execute('set threads=2');c.execute("set memory_limit='4GB'")
        inputs={name:rows(c,'read_parquet('+literal(root/'inputs'/(name+'.parquet'))+')') for name in ['monthly','song_dimension','song_labels','song_credits']}
        checked=0
        for period in ['month','year']:
            expected=python_marts(inputs['monthly'],inputs['song_dimension'],inputs['song_labels'],inputs['song_credits'],period)
            for name,keys in KEYS.items():
                actual=rows(c,'read_parquet('+literal(root/'expected'/period/(name+'.parquet'))+')')
                compare_rows(expected[name],actual,keys);checked+=1
    return {'tables_checked':checked,'mismatches':0,'source_access_required':False}


def freeze_sample(c,monthly_paths,audit,gold,seed_msids,output):
    output=Path(output).resolve();audit=Path(audit).resolve();gold=Path(gold).resolve()
    for source in [audit,gold,*map(Path,monthly_paths)]:
        path=source.resolve()
        if output==path or output in path.parents or path in output.parents:raise ValueError('overlapping baseline')
    output.mkdir(parents=True,exist_ok=False)
    inputs=output/'inputs';inputs.mkdir()
    c.read_parquet(str(gold/'song_dimension.parquet')).create_view('full_dimension',replace=True)
    c.execute('create or replace temp table chosen_keys as select distinct song_key from full_dimension where recording_msid in(select unnest(?))',[seed_msids])
    c.execute('create or replace temp table selected_dimension as select * from full_dimension semi join chosen_keys using(song_key)')
    if not c.execute('select count(*) from selected_dimension').fetchone()[0]:raise ValueError('empty sample')
    c.read_parquet(monthly_paths,hive_partitioning=True).create_view('full_monthly',replace=True)
    c.execute('create or replace temp table monthly as select user_name,recording_msid,listened_month,play_count from full_monthly semi join selected_dimension using(recording_msid)')
    count,plays=c.execute('select count(*),sum(play_count)::bigint from monthly').fetchone()
    if not count or count>250000:raise ValueError('sample must contain 1..250000 monthly rows')
    if c.execute('select count(*) from (select user_name,recording_msid,listened_month,count(*) n from monthly group by all having n<>1)').fetchone()[0]:raise ValueError('duplicate monthly key')
    c.execute('copy monthly to ? (format parquet,compression zstd)',[str(inputs/'monthly.parquet')])
    for table in ['reviewed','proposed_credits']:
        c.read_parquet(str(audit/(table+'.parquet'))).create_view('source_'+table,replace=True)
        c.execute('create or replace temp table '+table+' as select * from source_'+table+' semi join selected_dimension using(recording_msid)')
        c.execute('copy '+table+' to ? (format parquet,compression zstd)',[str(inputs/(table+'.parquet'))])
    build_dimension(c)
    for table in ['song_dimension','song_labels','song_credits']:
        c.read_parquet(str(gold/(table+'.parquet'))).create_view('full_'+table,replace=True)
        key='recording_msid' if table=='song_dimension' else 'song_key'
        c.execute('create or replace temp view scoped_original as select * from full_'+table+' semi join '+('selected_dimension' if key=='recording_msid' else 'chosen_keys')+' using('+key+')')
        actual=rows(c,table);expected=rows(c,'scoped_original')
        compare_rows(expected,actual,[key,'artist_mbid'] if table=='song_credits' else [key])
        c.execute('copy '+table+' to ? (format parquet,compression zstd)',[str(inputs/(table+'.parquet'))])
    saved={name:rows(c,'read_parquet('+literal(inputs/(name+'.parquet'))+')') for name in ['monthly','song_dimension','song_labels','song_credits']}
    reference_rows=0;checks=0;table_counts={}
    for period in ['month','year']:
        expected=python_marts(saved['monthly'],saved['song_dimension'],saved['song_labels'],saved['song_credits'],period)
        build_period(c,period)
        directory=output/'expected'/period;directory.mkdir(parents=True)
        table_counts[period]={}
        for table,keys in KEYS.items():
            path=directory/(table+'.parquet')
            c.execute('copy (select * from '+table+' order by '+','.join(keys)+') to ? (format parquet,compression zstd)',[str(path)])
            actual=rows(c,'read_parquet('+literal(path)+')')
            compare_rows(expected[table],actual,keys);checks+=1;table_counts[period][table]=len(actual)
        reference=output/'full_gold_reference'/period;reference.mkdir(parents=True)
        c.execute('copy (select * from read_parquet('+literal(gold/period/'song_metrics.parquet')+') semi join chosen_keys using(song_key)) to ? (format parquet,compression zstd)',[str(reference/'song_metrics.parquet')])
        original=rows(c,'read_parquet('+literal(reference/'song_metrics.parquet')+')')
        projection=[{k:v for k,v in row.items() if k not in ('listener_rank','listen_rank')} for row in original]
        local=[{k:v for k,v in row.items() if k not in ('listener_rank','listen_rank')} for row in expected['song_metrics']]
        compare_rows(local,projection,KEYS['song_metrics']);reference_rows+=len(original)
    return {'monthly_rows':count,'plays':plays,'source_msids':len(saved['song_dimension']),
            'song_keys':len(saved['song_labels']),'oracle_tables_checked':checks,'table_counts':table_counts,
            'full_song_reference_rows':reference_rows,'global_rank_compared':False,
            'comparison_scope':'subset ranks are recomputed; full-Gold ranks preserved separately',
            'production_applied':False}
