import json
import duckdb
import pytest

from flux.review.pilot import review_sample


ARTIST='847e8a0c-cc20-4213-9e16-975515c2a926'
RECORDING='33dd5ed8-15b9-46fb-8297-3ec1c8015cfc'


def fixture(c):
    c.execute('create table raw_recording(id varchar,gid varchar,name varchar,comment varchar,artist_credit varchar,video varchar)')
    c.execute('insert into raw_recording values (?,?,?,?,?,?)',['1',RECORDING,'Parler à mon père','','1','f'])
    c.execute('create table raw_recording_gid_redirect(gid varchar,new_id varchar)')
    c.execute('create table raw_artist(id varchar,gid varchar,name varchar,comment varchar,type varchar)')
    c.execute('insert into raw_artist values (?,?,?,?,?)',['1',ARTIST,'Céline Dion','','1'])
    c.execute('create table raw_artist_credit_name(artist_credit varchar,position varchar,artist varchar,name varchar)')
    c.execute("insert into raw_artist_credit_name values ('1','0','1','Celine Dion')")
    rows=[{'recording_msid':msid,'artist_name':'Céline Dion','track_name':title,
           'recording_mbid':RECORDING,'artist_mbids':[ARTIST],'mbid_source':'CLIENT','resolved_mbid':RECORDING}
          for msid,title in [('one','Parler à mon père'),('two','Parler a mon pere'),('three','PARLER A MON PERE')]]
    c.execute('create table metadata(recording_msid varchar,artist_name varchar,track_name varchar,recording_mbid varchar,artist_mbids varchar[],mbid_source varchar)')
    c.executemany('insert into metadata values (?,?,?,?,?,?)',[[r[k] for k in ('recording_msid','artist_name','track_name','recording_mbid','artist_mbids','mbid_source')] for r in rows])
    c.execute('insert into metadata values (?,?,?,?,?,?)',['three','Céline Dion','Parler a mon pere',None,None,'NONE'])
    c.execute('create table resolution(original_mbid varchar,resolved_mbid varchar)')
    c.execute('insert into resolution values (?,?)',[RECORDING,RECORDING])
    sample={'selected_cases':[{'key':['celine dion','parler a mon pere'],'rows':rows}],
            'official_recordings':{RECORDING:{'title':'Parler à mon père','comment':'','artist_credit':'1'}},
            'official_credits':{'1':[{'position':'0','artist_mbid':ARTIST,'name':'Céline Dion','comment':'','type':'1'}]}}
    return sample,[{'original':r} for r in rows]


def test_actual_workflow_keeps_human_pending_and_blocks_partial_msid(tmp_path):
    c=duckdb.connect()
    sample,preview=fixture(c)
    source=tmp_path/'source.parquet'
    c.execute("copy (select * from(values ('u','2026-08','one',2),('u','2026-08','two',3),('u','2026-08','three',7)) t(user_name,listened_month,recording_msid,play_count)) to ? (format parquet)",[str(source)])
    state={'silver_fingerprint':{'test':'pinned'},'official_snapshot':'snapshot','sample_sha256':'sample'}
    result=review_sample(c,sample,preview,state,[str(source)],tmp_path/'review')
    assert result['production_approved_candidates']==0
    assert result['latest_decisions']['approved']>0
    assert result['human_decisions']==0
    assert {r['source_msid'] for r in result['msid_scope']['mappings']}=={'one','two'}
    assert result['monthly_validation']['source_rows']==3
    assert result['monthly_validation']['after_rows']==2
    assert result['monthly_validation']['source_plays']==result['monthly_validation']['after_plays']==12
    decisions=[json.loads(line) for line in (tmp_path/'review/decisions.jsonl').read_text(encoding='utf-8').splitlines()]
    assert all(r['reviewer_kind']=='agent' for r in decisions)
    assert any(r['supersedes'] for r in decisions)
    assert len(json.loads((tmp_path/'review/facts.json').read_text(encoding='utf-8')))==3
    with pytest.raises(FileExistsError):review_sample(c,sample,preview,state,[str(source)],tmp_path/'review')


def test_official_snapshot_disagreement_stops_review(tmp_path):
    c=duckdb.connect()
    sample,preview=fixture(c)
    sample['official_recordings'][RECORDING]['title']='Unrelated recording'
    with pytest.raises(ValueError,match='official recording changed'):
        review_sample(c,sample,preview,{'official_snapshot':'snapshot'},[],tmp_path/'bad')
    assert not (tmp_path/'bad/decisions.jsonl').exists()
