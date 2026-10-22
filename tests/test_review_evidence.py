from dataclasses import asdict

from flux.review.evidence import build_evidence


A='00000000-0000-0000-0000-000000000001'
R='00000000-0000-0000-0000-000000000002'
OLD='00000000-0000-0000-0000-000000000003'


def inputs():
    original={'recording_msid':'msid','artist_name':'Artíst','track_name':'Café','recording_mbid':R,'artist_mbids':[A]}
    official={'id':'20','gid':R,'name':'Café','comment':'','artist_credit':'10','video':'f'}
    credits=[{'artist_mbid':A,'name':'Artíst'}]
    return original,official,credits


def build(original,official,credits,**kwargs):
    return build_evidence(original,official,credits,snapshot_id='test-snapshot',evidence_ref='unit-test',**kwargs)


def test_exact_name_and_canonical_title_are_verified_without_claiming_original_audio():
    raw,record,credits=inputs();raw['track_name']='Cafe'
    fact,proof=build(raw,record,credits)
    assert fact.official_artist_verified and fact.official_recording_verified and fact.version_verified
    assert fact.version_key=='recording:'+R
    assert proof['artist_check']=='official_name'
    assert fact.track_name=='Cafe' and raw['track_name']=='Cafe'


def test_accent_only_artist_requires_independent_official_id_corroboration():
    raw,record,credits=inputs();raw['artist_name']='Artist'
    fact,proof=build(raw,record,credits)
    assert fact.official_artist_verified and proof['artist_check']=='submitted_ids_and_credit'
    raw['artist_mbids']=None
    assert not build(raw,record,credits)[0].official_artist_verified
    raw['artist_mbids']=[OLD]
    assert not build(raw,record,credits)[0].official_artist_verified


def test_missing_mbid_and_unverified_redirect_never_inherit_a_target():
    raw,record,credits=inputs();raw['recording_mbid']=None
    assert build(raw,record,credits)[0].resolved_recording_mbid is None
    raw['recording_mbid']=OLD
    assert not build(raw,record,credits)[0].official_recording_verified
    verified=build(raw,record,credits,redirect={'gid':OLD,'new_id':'20'})[0]
    assert verified.official_recording_verified and verified.observed_recording_mbid==OLD


def test_live_remix_speed_and_local_conflicts_remain_distinct():
    raw,record,credits=inputs();record['comment']='live, 2020: Show'
    fact,_=build(raw,record,credits)
    assert fact.version_verified and fact.version_key=='live:'+R
    record['comment']='';raw['track_name']='Cafe (Sped Up)'
    assert not build(raw,record,credits)[0].version_verified
    record['name']='Cafe (Sped Up)'
    fact,_=build(raw,record,credits)
    assert fact.version_verified and fact.version_key=='sped_up:'+R
    record['video']='t'
    assert not build(raw,record,credits)[0].version_verified


def test_wrong_artist_unrecognized_version_and_scope_conflict_stay_visible():
    raw,record,credits=inputs()
    credits=[{'artist_mbid':OLD,'name':'Other Band'}]
    fact,_=build(raw,record,credits)
    assert not fact.official_artist_verified
    record['comment']='unclear alternate version'
    assert not build(raw,record,credits)[0].version_verified
    fact,proof=build(raw,record,credits,conflict=True)
    assert fact.conflict and proof['scope_conflict']
    assert asdict(fact)['artist_name']==raw['artist_name']
