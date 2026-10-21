import pytest

from flux.silver.name_preview import preview, review_groups


def row(msid,artist,title,reference=None):
    return {'recording_msid':msid,'artist_name':artist,'track_name':title,'resolved_mbid':reference}


def test_before_after_preserves_original_and_separates_alias_from_spelling():
    original=row('s','Beyonce\u0301','  I Can’t  ')
    result=preview(original)
    assert result['original']==original
    assert result['approved']['artist']=='beyonce'
    assert result['approved']['title']=="i can't"
    assert {c['rule'] for c in result['changes']}=={'NFC','lower','latin_accents','approved_characters','whitespace'}
    assert result['approved']['artist_alias_mbid']=='859d0860-d480-4efd-970c-c05d5f1776b8'
    assert preview(row('s','Beyonce','Irreplaceable'))['approved']['artist']=='beyonce'


def test_review_only_candidates_do_not_replace_approved_values():
    result=preview(row('s','Daft Punk','Phœnix'))
    assert result['approved']['title']=='phœnix'
    assert result['review_only']['latin_fold']['title']=='phoenix'
    assert preview(row('s','RM','ㅠㅠ'))['review_only']['nfkc']['title']=='ᅲᅲ'
    assert preview(row('s','a','Straße'))['review_only']['casefold']['title']=='strasse'
    assert preview(row('s','a','Σ'))['review_only']['latin_fold']['title']=='σ'
    assert preview(row('s','a','Song (Sped Up)'))['approved']['title']=='song (sped up)'


def test_equal_names_flag_reference_conflict_without_claiming_audio_identity():
    records=[preview(row('one','a','I Can’t','studio')),preview(row('two','a',"I Can't",'live'))]
    grouped=review_groups(records,'approved')
    assert grouped[0]['verdict']=='reference_conflict'
    assert grouped[0]['raw_name_pairs']==2
    unverified=[preview(row('one','a','Song','studio')),preview(row('two','A','Song'))]
    assert review_groups(unverified,'approved')[0]['verdict']=='same_reference_with_unverified'
    with pytest.raises(ValueError):review_groups(records,'not_a_mode')


def test_accent_fold_can_create_collisions_and_trace_matches_comparison():
    from flux.silver.name_candidates import normalize_text

    originals=[row('one','Résumé','Café (Remix)','first'),row('two','Resume','Cafe (Remix)','second')]
    records=[preview(r) for r in originals]
    assert review_groups(records,'approved')[0]['verdict']=='reference_conflict'
    for original,record in zip(originals,records):
        assert record['original']==original
        for field,source in (('artist','artist_name'),('title','track_name')):
            value=original[source]
            for change in record['changes']:
                if change['field']==field:
                    assert change['before']==value
                    value=change['after']
            assert value==record['approved'][field]==normalize_text(original[source])


def test_baseline_comparison_separates_text_changes_new_pairs_and_conflicts():
    from copy import deepcopy

    from flux.silver.name_preview import compare_baseline

    current=[preview(row('one','a','Café','first')),preview(row('two','a','Cafe','second'))]
    baseline=deepcopy(current)
    baseline[0]['approved']['title']='café'
    baseline[0]['approved']['candidate_key']=('artist_name','a','café')
    result=compare_baseline(current,baseline)
    assert result['changed_title_rows']==result['changed_candidate_key_rows']==1
    assert result['newly_equal_pairs']==result['new_reference_conflict_pairs']==[[0,1]]
    assert result['newly_equal_text_pairs']==result['new_text_reference_conflict_pairs']==[[0,1]]
    assert result['text_key_count_before']==2 and result['text_key_count_after']==1
    assert result['no_longer_equal_pairs']==[]
    with pytest.raises(ValueError,match='same sample'):
        compare_baseline(current,baseline[::-1])


def test_sample_driver_exports_previews_and_guards_inputs(tmp_path,monkeypatch):
    import importlib.util
    import json
    from pathlib import Path

    duckdb=pytest.importorskip('duckdb')
    spec=importlib.util.spec_from_file_location('preview_driver',Path(__file__).resolve().parents[1]/'scripts/investigations/preview_name_sample.py')
    driver=importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
    monkeypatch.setattr(driver,'EXTRA_CASES',{'examples':'true'})
    silver=tmp_path/'silver';prepared=tmp_path/'prepared';prepared.mkdir()
    for n in ('recording_metadata','monthly_user_recordings'):
        p=silver/n/'_delta_log';p.mkdir(parents=True)
        (p/'00000000000000000000.json').write_text('{}')
    rows=[{**row('one','a','I Can’t','studio'),'recording_mbid':'studio','recording_title':'Song','comment':'','credit_display':'a'},
          {**row('two','a',"I Can't",'live'),'recording_mbid':'live','recording_title':'Song','comment':'live','credit_display':'a'}]
    review=tmp_path/'review.json'
    review.write_text(json.dumps({'completed':True,'source_unchanged':True,'source_after':driver.fingerprint(silver),'cases':{'core':rows}}))
    pool=tmp_path/'pool.parquet'
    with duckdb.connect(str(prepared/'experiment.duckdb')) as c:
        c.execute('create table raw_recording(gid varchar,name varchar,comment varchar)')
        c.executemany('insert into raw_recording values (?,?,?)',[('studio','Song',''),('live','Song','live')])
        c.execute('create table samples(recording_msid varchar,artist_name varchar,track_name varchar,recording_mbid varchar,resolved_mbid varchar,recording_title varchar,comment varchar,credit_display varchar)')
        c.executemany('insert into samples values (?,?,?,?,?,?,?,?)',[[r[k] for k in driver.FIELDS] for r in rows])
        c.execute(f"copy samples to '{pool.as_posix()}' (format parquet)")
    output=tmp_path/'preview'
    result=driver.run(pool,review,prepared,silver,output)
    assert result['completed'] and result['sample_rows']==2
    assert result['group_verdict_counts']['approved']=={'reference_conflict':1}
    assert json.loads((output/'rows.json').read_text(encoding='utf-8'))[0]['original']['track_name']=='I Can’t'
    rerun=driver.run(pool,review,prepared,silver,tmp_path/'rerun',output/'rows.json')
    assert rerun['baseline_comparison']['newly_equal_pairs']==[]
    assert rerun['baseline_comparison']['changed_candidate_key_rows']==0
    assert rerun['baseline_comparison']['changed_rows']==[]
    assert str(output/'rows.json') in rerun['input_sha256']
    with pytest.raises(FileExistsError):driver.run(pool,review,prepared,silver,output)
    (silver/'recording_metadata/_delta_log/00000000000000000001.json').write_text('{}')
    with pytest.raises(ValueError,match='source'):driver.run(pool,review,prepared,silver,tmp_path/'stale')
    assert not (tmp_path/'stale').exists()
