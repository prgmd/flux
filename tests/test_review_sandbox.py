import pytest

from flux.review.journal import observation_id
from flux.review.sandbox import eligible_msid_mappings,validate_monthly_sandbox


def original(msid,mbid='recording'):
    return {'recording_msid':msid,'artist_name':'Artist','track_name':'Song','recording_mbid':mbid}


def mapping(row,target='same'):
    return {'observation_id':observation_id(row),'source_msid':row['recording_msid'],'recording_mbid':target,
            'official_artist_mbids':['artist'],'version_key':'recording:'+target}


def test_whole_msid_mapping_requires_every_metadata_variant_and_two_sources():
    one,two,three=original('one'),original('two'),original('three')
    metadata=[one,two,three,original('three',None)]
    result=eligible_msid_mappings(metadata,[mapping(one),mapping(two),mapping(three)])
    assert {m['source_msid'] for m in result['mappings']}=={'one','two'}
    assert result['excluded']==[{'source_msid':'three','reason':'unreviewed_metadata_variant'}]
    result=eligible_msid_mappings([one,two,original('two',None)],[mapping(one),mapping(two)])
    assert result['mappings']==[]
    assert {r['reason'] for r in result['excluded']}=={'unreviewed_metadata_variant','no_complete_peer'}


def test_monthly_snapshot_merge_preserves_plays_and_is_repeatable(tmp_path):
    duckdb=pytest.importorskip('duckdb')
    source=tmp_path/'source.parquet'
    database=str(tmp_path/'lookup.duckdb')
    c=duckdb.connect(database)
    c.execute("create table listens as select * from(values ('u','2026-08-01','one',2),('u','2026-08-01','two',3),('v','2026-08-01','one',1),('u','2026-08-01','held',7)) t(user_name,listened_month,recording_msid,play_count)")
    c.execute('copy listens to ? (format parquet)',[str(source)])
    c.close()
    before_database=(tmp_path/'lookup.duckdb').read_bytes()
    before_source=source.read_bytes()
    c=duckdb.connect(database,read_only=True)
    targets=[{'source_msid':'one','recording_mbid':'same','version_key':'recording:same'},
             {'source_msid':'two','recording_mbid':'same','version_key':'recording:same'}]
    result=validate_monthly_sandbox(c,[str(source)],['one','two','held'],targets,tmp_path/'sandbox')
    assert result['source_rows']==4 and result['after_rows']==3
    assert result['source_plays']==result['after_plays']==13
    assert result['oracle_mismatches']==0 and result['repeat_mismatches']==0
    held=c.execute('select play_count from read_parquet(?) where song_key=?',[str(tmp_path/'sandbox/after.parquet'),'msid:held']).fetchone()
    assert held==(7,)
    assert (tmp_path/'lookup.duckdb').read_bytes()==before_database
    assert source.read_bytes()==before_source
    with pytest.raises(FileExistsError):validate_monthly_sandbox(c,[str(source)],['one'],targets,tmp_path/'sandbox')
