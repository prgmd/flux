from collections import defaultdict
from pathlib import Path

from flux.review.journal import observation_id


def eligible_msid_mappings(metadata,observation_mappings):
    mapping={row['observation_id']:row for row in observation_mappings}
    groups=defaultdict(list)
    for row in metadata:
        groups[row['recording_msid']].append(row)
    possible={};excluded=[]
    for msid,rows in sorted(groups.items()):
        mapped=[mapping.get(observation_id(row)) for row in rows]
        if not all(mapped):
            excluded.append({'source_msid':msid,'reason':'unreviewed_metadata_variant'})
            continue
        targets={(row['recording_mbid'],row['version_key'],tuple(sorted(row['official_artist_mbids']))) for row in mapped}
        if len(targets)!=1:
            excluded.append({'source_msid':msid,'reason':'conflicting_observation_targets'})
            continue
        recording,version,artists=next(iter(targets))
        possible[msid]={'source_msid':msid,'recording_mbid':recording,'version_key':version,'official_artist_mbids':list(artists)}
    peers=defaultdict(list)
    for msid,row in possible.items():
        peers[(row['recording_mbid'],row['version_key'],tuple(row['official_artist_mbids']))].append(msid)
    accepted=[]
    for _,msids in sorted(peers.items()):
        if len(msids)<2:
            excluded.extend({'source_msid':msid,'reason':'no_complete_peer'} for msid in msids)
        else:
            accepted.extend(possible[msid] for msid in sorted(msids))
    return {'mappings':accepted,'excluded':sorted(excluded,key=lambda row:row['source_msid'])}


def validate_monthly_sandbox(connection,source_paths,source_msids,mappings,output):
    output=Path(output).resolve()
    for source in source_paths:
        path=Path(source).resolve()
        if output==path or output in path.parents or path in output.parents:
            raise ValueError('overlapping sandbox output')
    output.mkdir(parents=True,exist_ok=False)
    c=connection
    paths=','.join("'"+str(path).replace("'","''")+"'" for path in source_paths)
    c.execute('create or replace temp view review_monthly_source as select * from read_parquet(['+paths+'],hive_partitioning=true)')
    c.execute('create or replace temp table review_subset as select user_name,listened_month,recording_msid,play_count from review_monthly_source where recording_msid in(select unnest(?))',[source_msids])
    source_rows=c.execute('select count(*) from review_subset').fetchone()[0]
    if source_rows>1000000:
        raise ValueError('monthly sandbox exceeds pilot row limit')
    c.execute('copy review_subset to ? (format parquet)',[str(output/'before.parquet')])
    c.execute('create or replace temp table review_mapping(recording_msid varchar,song_key varchar)')
    mapping={row['source_msid']:'recording:'+row['recording_mbid']+'|version:'+row['version_key'] for row in mappings}
    if len(mapping)!=len(mappings):
        raise ValueError('duplicate MSID mapping')
    if mapping:c.executemany('insert into review_mapping values (?,?)',list(mapping.items()))
    query="""select user_name,listened_month,coalesce(m.song_key,'msid:'||s.recording_msid) as song_key,
        sum(play_count)::bigint as play_count from review_subset s left join review_mapping m using(recording_msid)
        group by user_name,listened_month,coalesce(m.song_key,'msid:'||s.recording_msid)"""
    c.execute('copy ('+query+') to ? (format parquet)',[str(output/'after.parquet')])
    c.execute('copy ('+query+') to ? (format parquet)',[str(output/'repeat.parquet')])
    expected=defaultdict(int)
    source_plays=0
    for user,month,msid,plays in c.execute('select user_name,listened_month,recording_msid,play_count from read_parquet(?)',[str(output/'before.parquet')]).fetchall():
        expected[(user,str(month),mapping.get(msid,'msid:'+msid))]+=int(plays)
        source_plays+=int(plays)
    actual={(u,str(month),key):int(plays) for u,month,key,plays in c.execute('select user_name,listened_month,song_key,play_count from read_parquet(?)',[str(output/'after.parquet')]).fetchall()}
    repeated={(u,str(month),key):int(plays) for u,month,key,plays in c.execute('select user_name,listened_month,song_key,play_count from read_parquet(?)',[str(output/'repeat.parquet')]).fetchall()}
    mismatches=sum(expected.get(key)!=actual.get(key) for key in set(expected)|set(actual))
    repeat=sum(actual.get(key)!=repeated.get(key) for key in set(actual)|set(repeated))
    if mismatches or repeat or source_plays!=sum(actual.values()):
        raise ValueError('sandbox validation mismatch')
    return {'source_rows':source_rows,'after_rows':len(actual),'source_plays':source_plays,
            'after_plays':sum(actual.values()),'oracle_mismatches':mismatches,'repeat_mismatches':repeat,
            'merged_msids':len(mapping),'production_applied':False}
