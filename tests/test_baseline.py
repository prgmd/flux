import json
from pathlib import Path

import duckdb
import pytest

from flux.validation.baseline import pin_delta,check_pin,freeze_sample,compare_rows,python_marts,replay_sample,copy_selection
from flux.validation.pinning import sha256,verify_bundle
from flux.validation.dumps import summarize_windows,dump_header
from flux.gold.monthly_marts import build_dimension,build_period


def delta_table(root):
    root.mkdir()
    (root/'_delta_log').mkdir()
    (root/'one.parquet').write_bytes(b'one')
    actions=[{'metaData':{'id':'source-id','schemaString':'{}','configuration':{}}},
             {'add':{'path':'one.parquet','size':3,'partitionValues':{}}}]
    (root/'_delta_log/00000000000000000000.json').write_text(''.join(json.dumps(a)+'\n' for a in actions))
    return root


def test_pin_keeps_version_and_rejects_source_advancement(tmp_path):
    root=delta_table(tmp_path/'delta')
    pinned=pin_delta(root)
    assert pinned['version']==0 and pinned['id']=='source-id'
    (root/'two.parquet').write_bytes(b'two')
    (root/'_delta_log/00000000000000000001.json').write_text(json.dumps({'add':{'path':'two.parquet','size':3,'partitionValues':{}}})+'\n')
    assert pin_delta(root,version=0)==pinned
    with pytest.raises(ValueError,match='source advanced'):check_pin(pinned)
    assert len(pin_delta(root)['files'])==2


def test_pin_rejects_broken_log_missing_file_and_path_escape(tmp_path):
    root=delta_table(tmp_path/'delta')
    log=root/'_delta_log/00000000000000000000.json'
    raw=log.read_text()
    log.write_text(raw+'{broken\n')
    with pytest.raises(ValueError):pin_delta(root)
    log.write_text(raw.replace('one.parquet','../outside.parquet'))
    with pytest.raises(ValueError,match='unsafe'):pin_delta(root)
    log.write_text(raw);(root/'one.parquet').unlink()
    with pytest.raises(FileNotFoundError):pin_delta(root)


def test_compare_rejects_duplicate_extra_and_changed_rows():
    expected=[{'key':'a','n':2,'share':.5}]
    compare_rows(expected,[{'key':'a','n':2,'share':.5+1e-14}],['key'])
    for wrong in [expected*2,[{'key':'a','n':3,'share':.5}],[],[{'key':'a','n':2,'share':.5,'extra':1}]]:
        with pytest.raises(ValueError):compare_rows(expected,wrong,['key'])


def sample_files(tmp_path,c):
    gold=tmp_path/'gold';audit=tmp_path/'audit';gold.mkdir();audit.mkdir()
    c.execute("""create table reviewed as select * from(values
        ('s1','r','song','Artist','Song','', 'matched_candidate'),
        ('s2','r','song','Artist','Song','', 'matched_candidate'),
        ('fast',NULL,'song (sped up remix)','Artist',NULL,NULL,'no_mbid'),
        ('unknown',NULL,'live forever','Other',NULL,NULL,'no_mbid'))
        t(recording_msid,recording_mbid,title,artist,recording_title,comment,status)""")
    c.execute("create table proposed_credits as select * from(values ('s1','a','Artist'),('s1','b','Guest'),('s2','a','Artist'),('s2','b','Guest')) t(recording_msid,artist_mbid,artist_name)")
    c.execute("""create table monthly as select * from(values
        ('u1','s1','2026-01',2),('u1','s2','2026-01',3),('u2','s1','2026-01',1),
        ('u1','s1','2026-02',4),('u2','fast','2026-01',7),('u3','unknown','2026-01',1))
        t(user_name,recording_msid,listened_month,play_count)""")
    source=tmp_path/'monthly.parquet'
    c.execute('copy monthly to ? (format parquet)',[str(source)])
    for table in ['reviewed','proposed_credits']:c.execute('copy '+table+' to ? (format parquet)',[str(audit/(table+'.parquet'))])
    build_dimension(c)
    for table in ['song_dimension','song_labels','song_credits']:c.execute('copy '+table+' to ? (format parquet)',[str(gold/(table+'.parquet'))])
    for period in ['month','year']:
        (gold/period).mkdir();build_period(c,period)
        for table in ['song_metrics','artist_metrics','artist_song_metrics','variant_metrics','listener_metrics','coverage','identity_coverage']:
            c.execute('copy '+table+' to ? (format parquet)',[str(gold/period/(table+'.parquet'))])
    return source,audit,gold


def test_self_contained_sample_matches_independent_oracle_and_full_song_reference(tmp_path):
    c=duckdb.connect();source,audit,gold=sample_files(tmp_path,c)
    result=freeze_sample(c,[str(source)],audit,gold,['s1','fast','unknown'],tmp_path/'baseline')
    assert result['monthly_rows']==6 and result['plays']==18
    assert result['source_msids']==4
    assert result['oracle_tables_checked']==14
    assert result['full_song_reference_rows']>0
    assert result['global_rank_compared'] is False
    assert (tmp_path/'baseline/inputs/monthly.parquet').exists()
    assert replay_sample(tmp_path/'baseline')['tables_checked']==14
    with pytest.raises(FileExistsError):freeze_sample(c,[str(source)],audit,gold,['s1'],tmp_path/'baseline')


def test_changed_existing_gold_is_not_accepted_as_baseline(tmp_path):
    c=duckdb.connect();source,audit,gold=sample_files(tmp_path,c)
    c.execute('create table wrong as select * replace(listens+1 as listens) from read_parquet(?)',[str(gold/'month/song_metrics.parquet')])
    c.execute('copy wrong to ? (format parquet)',[str(gold/'month/song_metrics.parquet')])
    with pytest.raises(ValueError,match='row mismatch'):
        freeze_sample(c,[str(source)],audit,gold,['s1'],tmp_path/'baseline')


def test_filtered_copy_keeps_list_parameter_separate_from_output_filename(tmp_path):
    c=duckdb.connect();path=tmp_path/"sample's metadata.parquet"
    copy_selection(c,"select * from (values ('one'),('two')) t(msid) where msid in(select unnest(?))",[['one']],path)
    assert c.execute('select * from read_parquet(?)',[str(path)]).fetchall()==[('one',)]


def test_global_rank_is_preserved_separately_from_subset_rank(tmp_path):
    c=duckdb.connect();source,audit,gold=sample_files(tmp_path,c)
    freeze_sample(c,[str(source)],audit,gold,['s1'],tmp_path/'baseline')
    local=c.execute('select listen_rank from read_parquet(?) where period_start=?',[str(tmp_path/'baseline/expected/month/song_metrics.parquet'),'2026-01-01']).fetchone()[0]
    global_rank=c.execute('select listen_rank from read_parquet(?) where period_start=?',[str(tmp_path/'baseline/full_gold_reference/month/song_metrics.parquet'),'2026-01-01']).fetchone()[0]
    assert local==1 and global_rank==2


def test_package_rejects_incomplete_or_tampered_artifact(tmp_path):
    data=tmp_path/'input.parquet';data.write_bytes(b'original')
    manifest={'completed':False,'artifacts':{'input.parquet':sha256(data)}}
    report=tmp_path/'result.json';report.write_text(json.dumps(manifest))
    with pytest.raises(ValueError,match='incomplete'):verify_bundle(tmp_path)
    manifest['completed']=True;report.write_text(json.dumps(manifest))
    verify_bundle(tmp_path)
    data.write_bytes(b'changed')
    with pytest.raises(ValueError,match='artifact changed'):verify_bundle(tmp_path)


def test_dump_window_gaps_and_overlaps_are_never_called_contiguous():
    records=[{'dump':str(i),'start':s,'end':e,'has_listen_record':True} for i,(s,e) in enumerate([
        ('2026-10-01T00:00:00Z','2026-10-02T00:00:00Z'),
        ('2026-10-02T00:00:00Z','2026-10-03T00:00:00Z'),
        ('2026-10-05T00:00:00Z','2026-10-06T00:00:00Z')])]
    summary=summarize_windows(records)
    assert len(summary['gaps'])==1 and summary['longest_contiguous_dumps']==2
    assert summary['continuous'] is False
    records.append({'dump':'empty','start':'2026-10-06T00:00:00Z','end':'2026-10-06T00:00:00Z','has_listen_record':False})
    assert summarize_windows(records)['excluded']==['empty']


def test_dump_header_reads_real_window_and_distinguishes_empty_payload(tmp_path):
    import io
    import tarfile

    zstd=pytest.importorskip('compression.zstd')
    for empty in [False,True]:
        stream=io.BytesIO()
        with tarfile.open(fileobj=stream,mode='w') as archive:
            for name,raw in [('START_TIMESTAMP',b'2026-10-01 00:00:00+00:00'),
                             ('END_TIMESTAMP',b'2026-10-02 00:00:00+00:00'),
                             ('listens/2026/10.listens',b'' if empty else b'{"user_name":"example"}\n')]:
                info=tarfile.TarInfo('dump/'+name);info.size=len(raw);archive.addfile(info,io.BytesIO(raw))
        path=tmp_path/('empty.tar.zst' if empty else 'real.tar.zst')
        path.write_bytes(zstd.compress(stream.getvalue()))
        entry={'dump':path.stem,'path':str(path),'size':path.stat().st_size}
        result=dump_header(entry)
        assert result['has_listen_record'] is (not empty)
        assert result['start']=='2026-10-01T00:00:00+00:00'
        assert result['archive_rehashed'] is False
        entry['size']+=1
        with pytest.raises(ValueError,match='size mismatch'):dump_header(entry)
