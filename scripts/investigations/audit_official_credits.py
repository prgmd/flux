import argparse
import hashlib
import json
import time
from pathlib import Path

from flux.common import deltalog
from flux.silver.musicbrainz import TABLE_COLUMNS


def review(c):
    c.execute("""create or replace temp macro typography(v) as
        translate(lower(trim(v)), '‘’‐‑', '''''--')""")
    c.execute("""create or replace table observed as select recording_msid,
        min(nullif(trim(recording_mbid),'')) recording_mbid,
        count(distinct nullif(trim(recording_mbid),'')) mbids,
        min(lower(trim(track_name))) title,min(lower(trim(artist_name))) artist,
        count(distinct lower(trim(track_name))) titles,count(distinct lower(trim(artist_name))) artists,
        count(*) filter(where nullif(trim(track_name),'') is null or nullif(trim(artist_name),'') is null) missing_names
        from metadata group by recording_msid""")
    c.execute("""create or replace temp table artist_match as select distinct o.recording_msid
        from observed o join recordings r using(recording_mbid) join credits c using(artist_credit)
        where typography(o.artist) in(typography(c.artist_name),typography(c.credit_name),typography(c.credit_display))""")
    c.execute("""create or replace table reviewed as select o.*,r.recording_title,r.artist_credit,r.comment,
        case when mbids=0 then 'no_mbid' when mbids>1 then 'mbid_conflict'
        when missing_names>0 or titles<>1 or artists<>1 then 'name_conflict'
        when r.recording_mbid is null then 'official_missing'
        when typography(o.title)<>typography(r.recording_title) then 'title_conflict'
        when m.recording_msid is null then 'artist_conflict'
        when coalesce(r.comment,'')<>'' or r.video then 'version_review'
        else 'matched_candidate' end status
        from observed o left join recordings r using(recording_mbid)
        left join artist_match m using(recording_msid)""")
    c.execute("""create or replace table proposed_credits as select distinct recording_msid,
        recording_mbid,c.artist_mbid,c.artist_name from reviewed r join credits c using(artist_credit)
        where status='matched_candidate'""")


def quoted(s):
    return "'"+str(s).replace("'","''")+"'"


def load_tsv(c, root, name, columns):
    schema='{'+','.join(quoted(n)+":'VARCHAR'" for n in columns)+'}'
    c.execute(f"""create or replace view raw_{name} as select * from read_csv(
        {quoted(root/name)},delim='\t',header=false,quote='',escape='',nullstr='\\N',
        columns={schema},auto_detect=false,strict_mode=true)""")


def run(snapshot, silver, output):
    import duckdb

    snapshot,silver,output=snapshot.resolve(),silver.resolve(),output.resolve()
    if any(output==p or p in output.parents or output in p.parents for p in (snapshot,silver)):
        raise ValueError('output overlaps input')
    snapshot_info=json.loads((snapshot/'result.json').read_text(encoding='utf-8'))
    if not snapshot_info['completed']:
        raise ValueError('snapshot incomplete')
    def fingerprint():
        return {name: hashlib.sha256(b''.join(p.read_bytes() for p in deltalog.commits(silver/name))).hexdigest()
                for name in ('recording_metadata','monthly_user_recordings')}
    before=fingerprint()
    output.mkdir(parents=True,exist_ok=False)
    c=duckdb.connect(str(output/'audit.duckdb'))
    c.execute('set threads=2'); c.execute("set memory_limit='8GB'")
    c.execute('set preserve_insertion_order=false')
    c.read_parquet(deltalog.active_paths(silver/'recording_metadata'),hive_partitioning=True).create_view('metadata')
    c.read_parquet(deltalog.active_paths(silver/'monthly_user_recordings'),hive_partitioning=True).create_view('monthly')
    start=time.monotonic()
    load_tsv(c,snapshot,'artist',TABLE_COLUMNS['artist'])
    load_tsv(c,snapshot,'artist_credit',('id','name','artist_count','ref_count','created','edits_pending','gid'))
    load_tsv(c,snapshot,'artist_credit_name',('artist_credit','position','artist','name','join_phrase'))
    load_tsv(c,snapshot,'recording',('id','gid','name','artist_credit','length','comment','edits_pending','last_updated','video'))
    c.execute("""create table recordings as select gid recording_mbid,name recording_title,
        artist_credit,comment,cast(video as boolean) video from raw_recording
        where gid in(select distinct recording_mbid from metadata where recording_mbid is not null)""")
    print('recordings selected',flush=True)
    c.execute("""create table credits as select n.artist_credit,a.gid artist_mbid,a.name artist_name,
        n.name credit_name,ac.name credit_display
        from raw_artist_credit_name n join raw_artist a on n.artist=a.id
        join raw_artist_credit ac on n.artist_credit=ac.id
        where n.artist_credit in(select distinct artist_credit from recordings)""")
    if c.execute('select count(*)-count(distinct recording_mbid) from recordings').fetchone()[0]:
        raise ValueError('duplicate official recording')
    print('credits ready',flush=True)
    review(c)
    if c.execute('select count(*)-count(distinct recording_msid) from reviewed').fetchone()[0]:
        raise ValueError('duplicate reviewed MSID')
    print('review classified',flush=True)
    stats=c.execute('''select status,count(*) monthly_rows,sum(play_count) plays,count(distinct user_name) users
        from monthly join reviewed using(recording_msid) group by status order by plays desc''').fetchall()
    if sum(row[2] for row in stats)!=c.execute('select sum(play_count) from monthly').fetchone()[0]:
        raise ValueError('coverage lost plays')
    for name in ('reviewed','proposed_credits'):
        c.execute(f'copy {name} to {quoted(output/(name+".parquet"))} (format parquet)')
    after=fingerprint()
    if before!=after:
        raise ValueError('Silver changed during review')
    result={'completed':True,'source_release':snapshot_info['source_release'],'stats':stats,
            'source_before':before,'source_after':after,'source_unchanged':True,
            'msid_states':c.execute('select status,count(*) from reviewed group by status').fetchall(),
            'recordings':c.execute('select count(*) from recordings').fetchone()[0],
            'credit_rows':c.execute('select count(*) from proposed_credits').fetchone()[0],
            'seconds':round(time.monotonic()-start,2),'applied':False}
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    c.close(); print(json.dumps(result),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--snapshot',type=Path,required=True); p.add_argument('--silver',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args(); run(a.snapshot,a.silver,a.output)
