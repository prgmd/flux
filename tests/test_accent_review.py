import pytest

from flux.silver.accent_review import accent_groups, analyze_accent_group


def row(artist,title,reference=None,msid='sample'):
    return {'artist_name':artist,'track_name':title,'resolved_mbid':reference,'recording_msid':msid}


def test_new_conflicts_are_between_previously_separate_accent_buckets():
    rows=[row('Björk','Jóga','studio'),row('Björk','Jóga','live'),row('Bjork','Joga','studio')]
    result=analyze_accent_group(rows)
    assert result['verdict']=='new_reference_conflict'
    assert result['new_conflicting_reference_pairs']==[['live','studio']]
    assert result['shared_reference_mbids']==['studio']
    assert result['previous_key_count']==2


def test_existing_conflict_is_not_blamed_on_accent_removal():
    result=analyze_accent_group([row('bôa','Duvet','studio'),row('bôa','Duvet','short'),row('boa','Duvet')])
    assert result['verdict']=='existing_reference_conflict'
    assert result['new_conflicting_reference_pairs']==[]
    assert result['unknown_reference_rows']==1


def test_shared_unknown_and_missing_references_have_different_verdicts():
    rows=[row('Céline','Parler à mon père','same'),row('Celine','Parler a mon pere','same')]
    assert analyze_accent_group(rows)['verdict']=='same_reference_candidate'
    assert analyze_accent_group(rows+[row('Céline','Parler a mon pere')])['verdict']=='same_reference_with_unverified'
    assert analyze_accent_group([row('a','Café'),row('a','Cafe')])['verdict']=='reference_missing'
    with pytest.raises(ValueError):analyze_accent_group([row('a','Café'),row('a','Other')])


def test_only_accent_introduced_links_are_selected_and_versions_stay_separate():
    rows=[row('Björk','Jóga','one'),row('Bjo\u0308rk','Jo\u0301ga','one'),
          row('Björk','Joga','one'),row('Björk','Joga (Sped Up)','two'),
          row('MØ','Song'),row('MO','Song')]
    groups=accent_groups(rows)
    assert len(groups)==1 and groups[0]['key']==['bjork','joga']
    assert len(groups[0]['rows'])==3
    assert rows[0]['artist_name']=='Björk'


def test_sample_driver_checks_official_references_and_guards_source(tmp_path,monkeypatch):
    import importlib.util
    import json
    from pathlib import Path

    duckdb=pytest.importorskip('duckdb')
    spec=importlib.util.spec_from_file_location('accent_driver',Path(__file__).resolve().parents[1]/'scripts/investigations/review_accent_expansion.py')
    driver=importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
    monkeypatch.setattr(driver,'ARTIST_FILTERS',['bjork'])
    monkeypatch.setattr(driver,'CASES',[('bjork','joga')])
    monkeypatch.setattr(driver,'CONTROLS',())
    monkeypatch.setattr(driver,'ARTIST_BOUNDARIES',())
    silver=tmp_path/'silver';prepared=tmp_path/'prepared';prepared.mkdir()
    for name in ('recording_metadata','monthly_user_recordings'):
        log=silver/name/'_delta_log';log.mkdir(parents=True)
        (log/'00000000000000000000.json').write_text('{}')
    (prepared/'result.json').write_text(json.dumps({'source_before':{'silver':driver.fingerprint(silver)}}))
    with duckdb.connect(str(prepared/'experiment.duckdb')) as c:
        c.execute('create table metadata(recording_msid varchar,artist_name varchar,track_name varchar,recording_mbid varchar)')
        c.executemany('insert into metadata values (?,?,?,?)',[('one','Björk','Jóga','studio'),('two','Bjork','Joga','studio'),('three','Björk','Jóga','live')])
        c.execute("create table resolution as select 'studio' original_mbid,'studio' resolved_mbid union all select 'live','live'")
        c.execute("create table recordings as select 'studio' recording_mbid,'Jóga' recording_title,'1' artist_credit,'' as comment union all select 'live','Jóga','1','live'")
        c.execute("create table credit_displays as select '1' artist_credit,'Björk' credit_display")
        c.execute('create table raw_recording as select recording_mbid as gid,recording_title as name,artist_credit,comment from recordings')
        c.execute("create table raw_artist_credit_name as select '1' as artist_credit,'0' as position,'1' as artist")
        c.execute("create table raw_artist as select '1' id,'artist-id' gid,'Björk' as name,'' as comment,'1' as type")
    output=tmp_path/'output'
    result=driver.run(prepared,silver,output)
    assert result['completed'] and result['source_unchanged'] and result['official_recordings_checked']==2
    assert result['selected_cases'][0]['verdict']=='new_reference_conflict'
    assert result['official_credits']['1'][0]['artist_mbid']=='artist-id'
    assert not result['production_applied'] and not result['full_corpus_preprocessed']
    assert (output/'before-after.md').is_file()
    with pytest.raises(FileExistsError):driver.run(prepared,silver,output)
    with duckdb.connect(str(prepared/'experiment.duckdb')) as c:
        c.execute("update raw_recording set comment='changed official evidence' where gid='live'")
    with pytest.raises(ValueError,match='official recording mismatch'):
        driver.run(prepared,silver,tmp_path/'mismatched')
    assert json.loads((tmp_path/'mismatched/result.json').read_text(encoding='utf-8'))['completed'] is False
    (silver/'recording_metadata/_delta_log/00000000000000000001.json').write_text('{}')
    with pytest.raises(ValueError,match='source'):driver.run(prepared,silver,tmp_path/'stale')
    assert not (tmp_path/'stale').exists()
