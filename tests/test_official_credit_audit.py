import importlib.util
from pathlib import Path
import pytest

duckdb=pytest.importorskip('duckdb')
spec=importlib.util.spec_from_file_location('credits',Path(__file__).resolve().parents[1]/'scripts/investigations/audit_official_credits.py')
credits=importlib.util.module_from_spec(spec)
spec.loader.exec_module(credits)


def test_review_rejects_conflicts_and_uses_credit_not_raw_artist_ids():
    with duckdb.connect() as c:
        c.execute("""create table metadata as select * from (values
        ('ok','Song','Main','r'),('ok','Song','Main',NULL),
        ('badtitle','Other','Main','r'),('speed','Song (Sped Up)','Main','r'),('badartist','Song','Wrong','r'),
        ('live','Song','Main','live'),('missing','Song','Main','gone'),
        ('none','Song','Main',NULL),('conflict','Song','Main','r'),('conflict','Song','Main','live'))
        t(recording_msid,track_name,artist_name,recording_mbid)""")
        c.execute("""create table recordings as select * from (values
        ('r','Song','c','',false),('live','Song','c','live performance',false))
        t(recording_mbid,recording_title,artist_credit,comment,video)""")
        c.execute("""create table credits as select * from (values
        ('c','a','Main','Main','Main feat. Guest'),('c','b','Guest','Guest','Main feat. Guest'))
        t(artist_credit,artist_mbid,artist_name,credit_name,credit_display)""")
        credits.review(c)
        states=dict(c.execute('select recording_msid,status from reviewed').fetchall())
        assert states=={'ok':'matched_candidate','badtitle':'title_conflict','speed':'title_conflict','badartist':'artist_conflict',
                       'live':'version_review','missing':'official_missing','none':'no_mbid','conflict':'mbid_conflict'}
        assert c.execute("select artist_mbid from proposed_credits where recording_msid='ok' order by 1").fetchall()==[('a',),('b',)]
        assert c.execute('select count(*) from proposed_credits').fetchone()[0]==2


def test_typography_requires_same_official_id_and_preserves_versions():
    with duckdb.connect() as c:
        c.execute("""create table metadata as select * from (values
        ('ok','Don''t Go','alt-j','r'),('absent','Don''t Go','alt-j','missing'),
        ('speed','Don''t Go (Sped Up)','alt-j','r'),('clean','Don''t Go','alt-j','clean'),
        ('live','Don''t Go','alt-j','live')) t(recording_msid,track_name,artist_name,recording_mbid)""")
        c.execute("""create table recordings as select * from (values
        ('r','Don’t Go','c','',false),('clean','Don’t Go','c','censored',false),
        ('live','Don’t Go','c','live',false)) t(recording_mbid,recording_title,artist_credit,comment,video)""")
        c.execute("""create table credits as select 'c' artist_credit,'a' artist_mbid,
        'alt‐J' artist_name,'alt‐J' credit_name,'alt‐J' credit_display""")
        credits.review(c)
        assert dict(c.execute('select recording_msid,status from reviewed').fetchall())=={
            'ok':'matched_candidate','absent':'official_missing','speed':'title_conflict',
            'clean':'version_review','live':'version_review'}
        assert c.execute("select title from reviewed where recording_msid='ok'").fetchone()[0]=="don't go"
