import pytest

duckdb = pytest.importorskip('duckdb')
from flux.gold.artist_attribution import classify
from flux.gold.monthly_marts import build_dimension, build_period


def test_redirect_and_artist_acceptance_do_not_merge_versions():
    with duckdb.connect() as c:
        c.execute("""create table prior as select * from (values
            ('base','r',1,'song','main','Song','','matched_candidate'),
            ('fast','old',1,'song (sped up)','main',NULL,NULL,'official_missing'),
            ('remix','mix',1,'song (remix)','main','Song (Remix)','remix','version_review'),
            ('bad','other',1,'song','main','Song','','artist_conflict'),
            ('wrong','r',1,'other song','main','Song','','title_conflict'),
            ('conflict','r',2,'song','main','Song','','mbid_conflict'),
            ('unresolved','gone',1,'song','main',NULL,NULL,'official_missing'),
            ('absent',NULL,0,'song','main',NULL,NULL,'no_mbid'))
            t(recording_msid,recording_mbid,mbids,title,artist,recording_title,comment,status)""")
        c.execute("create table resolution as select * from (values ('r','r'),('old','speed'),('mix','mix'),('other','other')) t(original_mbid,resolved_mbid)")
        c.execute("""create table recordings as select * from (values
            ('r','Song','1','',false),('speed','Song (Sped Up)','1','sped up',false),
            ('mix','Song (Remix)','1','remix',false),('other','Song','2','',false))
            t(recording_mbid,recording_title,artist_credit,comment,video)""")
        c.execute("create table credits as select * from (values ('1','a','Main','Main','Main'),('2','b','Other','Other','Other')) t(artist_credit,artist_mbid,artist_name,credit_name,credit_display)")
        classify(c)
        assert c.execute("select resolved_mbid,artist_status from reviewed where recording_msid='fast'").fetchone()==('speed','accepted')
        assert c.execute("select count(*) from proposed_credits").fetchone()[0]==3
        assert c.execute("select artist_status from reviewed where recording_msid='unresolved'").fetchone()[0]=='official_missing'
        assert c.execute("select artist_status from reviewed where recording_msid='absent'").fetchone()[0]=='no_mbid'
        build_dimension(c)
        assert c.execute("select song_key,variant from song_dimension where recording_msid='fast'").fetchone()==('msid:fast','sped_up')
        assert c.execute("select song_key,variant from song_dimension where recording_msid='remix'").fetchone()==('msid:remix','remix')
        c.execute("create table monthly as select * from (values ('u','base','2026-01',2),('u','fast','2026-01',3),('v','remix','2026-01',4)) t(user_name,recording_msid,listened_month,play_count)")
        build_period(c,'month')
        assert c.execute('select listens,listeners from artist_metrics').fetchone()==(9,2)
        assert c.execute('select count(*),sum(listens) from song_metrics').fetchone()==(3,9)


def test_redirect_snapshot_driver_and_stale_source(tmp_path):
    import hashlib
    import importlib.util
    import json
    from pathlib import Path
    from flux.silver.musicbrainz import TABLE_COLUMNS

    spec=importlib.util.spec_from_file_location('resolve_driver',Path(__file__).resolve().parents[1]/'scripts/investigations/resolve_artist_attribution.py')
    driver=importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
    silver=tmp_path/'silver';audit=tmp_path/'audit';snapshot=tmp_path/'snapshot';redirects=tmp_path/'redirects'
    for p in (audit,snapshot,redirects):p.mkdir()
    fp={}
    for n in ('recording_metadata','monthly_user_recordings'):
        log=silver/n/'_delta_log';log.mkdir(parents=True)
        content=json.dumps({'add':{'path':'part.parquet','partitionValues':{}}}).encode()
        (log/'00000000000000000000.json').write_bytes(content)
        fp[n]=hashlib.sha256(content).hexdigest()
    with duckdb.connect() as c:
        c.execute("create table prior as select 's' recording_msid,'old' recording_mbid,1 mbids,'song' title,'main' artist,NULL::varchar recording_title,NULL::varchar as comment,'official_missing' status")
        c.execute(f"copy prior to '{(audit/'reviewed.parquet').as_posix()}' (format parquet)")
        c.execute(f"copy (select 'u' user_name,'s' recording_msid,'2026-01' listened_month,3 play_count) to '{(silver/'monthly_user_recordings/part.parquet').as_posix()}' (format parquet)")
    (audit/'result.json').write_text(json.dumps({'completed':True,'source_unchanged':True,'source_after':fp}))
    (snapshot/'result.json').write_text(json.dumps({'completed':True,'source_release':'test'}))
    def tsv(path,rows):path.write_text('\n'.join('\t'.join(row) for row in rows)+'\n')
    tsv(redirects/'recording_gid_redirect',[['old','1','2026-01-01']])
    (redirects/'result.json').write_text(json.dumps({'completed':True,'source_release':'test','tables':{
        'recording_gid_redirect':{'sha256':hashlib.sha256((redirects/'recording_gid_redirect').read_bytes()).hexdigest()}}}))
    tsv(snapshot/'recording',[['1','current','Song','10','1000','album version','0','2026-01-01','f']])
    artist={'id':'1','gid':'a','name':'Main'}
    tsv(snapshot/'artist',[[artist.get(n,'\\N') for n in TABLE_COLUMNS['artist']]])
    tsv(snapshot/'artist_credit',[['10','Main','1','1','2026-01-01','0','credit-gid']])
    tsv(snapshot/'artist_credit_name',[['10','0','1','Main','']])
    out=tmp_path/'out'
    driver.run(silver,audit,snapshot,redirects,out)
    result=json.loads((out/'result.json').read_text())
    assert result['totals']==[3,0,3,3]
    with duckdb.connect() as c:
        assert c.execute(f"select recording_mbid,resolved_mbid,artist_status,status from read_parquet('{(out/'reviewed.parquet').as_posix()}')").fetchone()==('old','current','accepted','official_missing')
    with pytest.raises(FileExistsError):driver.run(silver,audit,snapshot,redirects,out)
    (silver/'recording_metadata/_delta_log/00000000000000000001.json').write_text('{}')
    with pytest.raises(ValueError,match='stale'):driver.run(silver,audit,snapshot,redirects,tmp_path/'stale')
