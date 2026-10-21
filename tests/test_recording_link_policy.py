from dataclasses import asdict,replace

import pytest

from flux.silver.recording_link_policy import RecordingEvidence,assess_pair


ARTIST='00000000-0000-0000-0000-000000000001'
GUEST='00000000-0000-0000-0000-000000000002'
RECORDING='00000000-0000-0000-0000-000000000003'
OTHER='00000000-0000-0000-0000-000000000004'


def confirmed(**changes):
    original=RecordingEvidence(source_msid='first',artist_name='Artíst',track_name='Café',
        official_artist_mbids=(ARTIST,),observed_recording_mbid=RECORDING,resolved_recording_mbid=RECORDING,
        official_artist_verified=True,official_recording_verified=True,
        version_key='original',version_verified=True,snapshot_id='test-snapshot',evidence_ref='test-evidence:one')
    return replace(original,**changes)


@pytest.mark.parametrize('version',['original','live:show-one','remix:mix-one','sped_up:edit-one'])
def test_matching_official_evidence_links_only_within_the_same_verified_version(version):
    left=confirmed(version_key=version)
    right=confirmed(source_msid='second',artist_name='Artist',track_name='Cafe',version_key=version)
    assert assess_pair(left,right)['status']=='eligible'
    assert assess_pair(right,left)==assess_pair(left,right)


@pytest.mark.parametrize('left_version,right_version',[
    ('original','live:show-one'),('original','remix:mix-one'),('original','sped_up:edit-one'),
    ('live:show-one','live:show-two'),('remix:mix-one','remix:mix-two')])
def test_same_ids_cannot_override_verified_version_conflicts(left_version,right_version):
    decision=assess_pair(confirmed(version_key=left_version),confirmed(source_msid='second',version_key=right_version))
    assert decision['status']=='keep_separate' and decision['reason']=='version_conflict'


@pytest.mark.parametrize('change,reason',[
    ({'resolved_recording_mbid':OTHER},'recording_id_conflict'),
    ({'official_artist_mbids':(OTHER,)},'artist_id_conflict'),
    ({'official_artist_mbids':(ARTIST,GUEST)},'artist_id_conflict')])
def test_equal_names_or_partial_credit_overlap_cannot_override_id_conflicts(change,reason):
    decision=assess_pair(confirmed(),confirmed(source_msid='second',**change))
    assert decision['status']=='keep_separate' and decision['reason']==reason


@pytest.mark.parametrize('change',[
    {'resolved_recording_mbid':None},{'official_artist_mbids':()},
    {'resolved_recording_mbid':'not-a-uuid'},{'official_artist_verified':False},
    {'official_recording_verified':False},{'version_key':None},{'version_verified':False},
    {'evidence_ref':None},{'snapshot_id':None},{'conflict':True}])
def test_missing_or_unverified_evidence_stays_on_hold_without_inheriting_other_rows(change):
    left=confirmed();right=confirmed(source_msid='second',**change)
    before=(asdict(left),asdict(right))
    assert assess_pair(left,right)['status']=='hold'
    assert (asdict(left),asdict(right))==before


def test_credit_order_does_not_change_identity_but_snapshot_must_match():
    left=confirmed(official_artist_mbids=(ARTIST,GUEST))
    right=confirmed(source_msid='second',official_artist_mbids=(GUEST,ARTIST))
    assert assess_pair(left,right)['status']=='eligible'
    assert assess_pair(left,replace(right,snapshot_id='another-snapshot'))['status']=='hold'


def test_version_words_are_not_removed_to_create_a_candidate():
    right=confirmed(source_msid='second',track_name='Cafe (Sped Up)')
    assert assess_pair(confirmed(),right)['status']=='not_candidate'
    assert assess_pair(confirmed(),replace(right,track_name='Cafe (Remix)'))['status']=='not_candidate'
    assert assess_pair(confirmed(),replace(right,track_name='Cafe (Live)'))['status']=='not_candidate'


def test_original_mbid_and_verified_redirect_provenance_are_retained():
    left=confirmed();right=confirmed(source_msid='second')
    assert 'observed_recording_mbid' in asdict(left)
    assert assess_pair(left,replace(right,observed_recording_mbid=None))['status']=='hold'
    redirected=replace(right,observed_recording_mbid=OTHER)
    assert assess_pair(left,redirected)['status']=='eligible'
    assert redirected.observed_recording_mbid==OTHER
