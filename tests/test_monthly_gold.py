import pytest

duckdb = pytest.importorskip('duckdb')
from flux.gold.monthly_marts import build_dimension, build_period


def fixture(c):
    c.execute("""create table reviewed as select * from (values
        ('s1','r','song','main','Song','', 'matched_candidate'),
        ('s2','r','song','main','Song','', 'matched_candidate'),
        ('fast','r','song (sped up remix)','main','Song','', 'title_conflict'),
        ('unknown',NULL,'live forever','other',NULL,NULL,'no_mbid'))
        t(recording_msid,recording_mbid,title,artist,recording_title,comment,status)""")
    c.execute("""create table proposed_credits as select * from (values
        ('s1','a','Main'),('s1','b','Guest'),('s2','a','Main'),('s2','b','Guest'))
        t(recording_msid,artist_mbid,artist_name)""")
    c.execute("""create table monthly as select * from (values
        ('u1','s1','2026-01',2),('u1','s2','2026-01',3),
        ('u2','s1','2026-01',1),('u1','s1','2026-02',4),
        ('u2','fast','2026-01',7),('u3','unknown','2026-01',1))
        t(user_name,recording_msid,listened_month,play_count)""")


def test_monthly_and_yearly_gold_keep_versions_and_distinct_users():
    with duckdb.connect() as c:
        fixture(c)
        build_dimension(c)
        assert c.execute("select variant from song_dimension where recording_msid='fast'").fetchone()[0]=='sped_up+remix'
        assert c.execute("select variant from song_dimension where recording_msid='unknown'").fetchone()[0]=='unmarked'
        build_period(c,'month')
        row=c.execute("select listens,listeners,top_user_listens,listener_rank from song_metrics where song_key='mbid:r' and period_start='2026-01-01'").fetchone()
        assert row==(6,2,5,1)
        assert c.execute("select listens,listeners,listener_rank from artist_metrics where period_start='2026-01-01' order by artist_name").fetchall()==[(6,2,1),(6,2,1)]
        assert c.execute('select sum(listens) from song_metrics').fetchone()[0]==18
        assert c.execute('select sum(linked_listens) from coverage').fetchone()[0]==10
        assert c.execute('select max(song_listen_share) from artist_song_metrics').fetchone()[0]==1
        assert c.execute("select listeners from variant_metrics where variant='sped_up+remix'").fetchone()[0]==1
        build_period(c,'year')
        assert c.execute("select listens,listeners,top_user_listens from song_metrics where song_key='mbid:r'").fetchone()==(10,2,9)
        assert c.execute('select period_users,period_listens from coverage').fetchone()==(3,18)


def test_duplicate_credits_do_not_multiply_plays():
    with duckdb.connect() as c:
        fixture(c)
        c.execute('insert into proposed_credits select * from proposed_credits')
        build_dimension(c)
        build_period(c,'year')
        assert c.execute("select listens from artist_metrics where artist_mbid='a'").fetchone()[0]==10


def test_concentration_and_invalid_period():
    with duckdb.connect() as c:
        fixture(c)
        build_dimension(c)
        with pytest.raises(ValueError,match='unsupported'):
            build_period(c,'week')
        build_period(c,'year')
        hhi,effective=c.execute("select concentration_hhi,effective_listeners from song_metrics where song_key='mbid:r'").fetchone()
        assert hhi==pytest.approx(.82)
        assert effective==pytest.approx(1/.82)


def test_separate_version_labels_and_no_plain_title_keyword_match():
    with duckdb.connect() as c:
        fixture(c)
        c.execute("""insert into reviewed values
            ('slow',NULL,'song (slowed + reverb)','main',NULL,NULL,'no_mbid'),
            ('clean',NULL,'song [clean]','main',NULL,NULL,'no_mbid'),
            ('explicit',NULL,'song [explicit]','main',NULL,NULL,'no_mbid'),
            ('live',NULL,'song (live) [remastered]','main',NULL,NULL,'no_mbid')""")
        build_dimension(c)
        rows=dict(c.execute("select recording_msid,variant from song_dimension").fetchall())
        assert rows['slow']=='slowed+effect'
        assert rows['clean']=='clean'
        assert rows['explicit']=='explicit'
        assert rows['live']=='live+remaster'
        assert rows['unknown']=='unmarked'


def test_snapshot_driver_exports_and_rejects_stale_audit(tmp_path):
    import importlib.util
    import json
    from pathlib import Path
    spec=importlib.util.spec_from_file_location('gold_driver',Path(__file__).resolve().parents[1]/'scripts/build_monthly_gold.py')
    driver=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(driver)
    silver=tmp_path/'silver'; audit=tmp_path/'audit'; audit.mkdir()
    for name in ('recording_metadata','monthly_user_recordings'):
        log=silver/name/'_delta_log';log.mkdir(parents=True)
        (log/'00000000000000000000.json').write_text(json.dumps({'add':{'path':'part.parquet','partitionValues':{}}}))
    with duckdb.connect() as c:
        fixture(c)
        c.execute(f"copy monthly to '{(silver/'monthly_user_recordings/part.parquet').as_posix()}' (format parquet)")
        for name in ('reviewed','proposed_credits'):
            c.execute(f"copy {name} to '{(audit/(name+'.parquet')).as_posix()}' (format parquet)")
    (audit/'result.json').write_text(json.dumps({'completed':True,'source_unchanged':True,'source_after':driver.fingerprint(silver)}))
    driver.run(silver,audit,tmp_path/'out')
    result=json.loads((tmp_path/'out/result.json').read_text())
    assert result['completed'] and result['listens']==18
    assert len(list((tmp_path/'out/month').glob('*.parquet')))==7
    assert len(list((tmp_path/'out/year').glob('*.parquet')))==7
    with duckdb.connect() as c:
        assert c.execute(f"select typeof(listens) from read_parquet('{(tmp_path/'out/month/song_metrics.parquet').as_posix()}') limit 1").fetchone()[0]=='BIGINT'
    finalize_spec=importlib.util.spec_from_file_location('gold_finalize',Path(__file__).resolve().parents[1]/'scripts/finalize_monthly_gold.py')
    finalizer=importlib.util.module_from_spec(finalize_spec)
    finalize_spec.loader.exec_module(finalizer)
    float_source=tmp_path/'out/month/song_metrics.parquet'
    float_copy=tmp_path/'float.parquet'
    with duckdb.connect() as c:
        c.execute(f"copy (select * replace(listens::double as listens) from read_parquet('{float_source.as_posix()}')) to '{float_copy.as_posix()}' (format parquet)")
    float_copy.replace(float_source)
    finalizer.finalize(tmp_path/'out',tmp_path/'final')
    with duckdb.connect(str(tmp_path/'final/gold.duckdb'),read_only=True) as c:
        assert c.execute('select sum(listens) from month_song_metrics').fetchone()[0]==18
        assert c.execute('select sum(listens) from year_song_metrics').fetchone()[0]==18
        assert c.execute('select typeof(listens) from month_song_metrics limit 1').fetchone()[0]=='BIGINT'
    verify_spec=importlib.util.spec_from_file_location('gold_verify',Path(__file__).resolve().parents[1]/'scripts/verify_monthly_gold.py')
    verifier=importlib.util.module_from_spec(verify_spec)
    verify_spec.loader.exec_module(verifier)
    checked=verifier.verify_samples(silver,audit,tmp_path/'out',['r'],['a'])
    assert checked['completed'] and checked['periods_checked']==6
    audit_manifest=(audit/'result.json').read_bytes()
    (audit/'result.json').write_bytes(audit_manifest+b' ')
    with pytest.raises(ValueError,match='audit mismatch'):
        verifier.verify_samples(silver,audit,tmp_path/'out',['r'],[])
    (audit/'result.json').write_bytes(audit_manifest)
    damaged=tmp_path/'out/year/song_metrics.parquet'
    replacement=tmp_path/'damaged.parquet'
    with duckdb.connect() as c:
        c.execute(f"copy (select * replace(listens+1 as listens) from read_parquet('{damaged.as_posix()}')) to '{replacement.as_posix()}' (format parquet)")
    replacement.replace(damaged)
    with pytest.raises(ValueError,match='metric mismatch'):
        verifier.verify_samples(silver,audit,tmp_path/'out',['r'],[])
    assert not json.loads((tmp_path/'out/sample-verification.json').read_text())['completed']
    with pytest.raises(FileExistsError):
        driver.run(silver,audit,tmp_path/'out')
    (silver/'recording_metadata/_delta_log/00000000000000000001.json').write_text('{}')
    with pytest.raises(ValueError,match='does not match'):
        driver.run(silver,audit,tmp_path/'stale')
    assert not (tmp_path/'stale').exists()
