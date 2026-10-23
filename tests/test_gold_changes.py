from copy import deepcopy

import pytest

from test_quality_contract import dataset
from flux.validation.gold_changes import affected_periods,cdf_periods


def test_no_change_and_repeated_input_have_no_affected_periods():
    before=dataset()
    assert affected_periods(before,deepcopy(before),policy_before='v1',policy_after='v1')['months']==[]


def test_month_and_year_move_and_last_key_deletion_include_old_period():
    before=dataset();after=deepcopy(before)
    after['monthly'][0]['listened_month']='2027-03'
    after['monthly']=[row for row in after['monthly'] if row['listened_month']!='2026-02']
    result=affected_periods(before,after,policy_before='v1',policy_after='v1')
    assert result['months']==['2026-01','2026-02','2027-03']
    assert result['years']==['2026','2027']


def test_mapping_or_label_change_expands_to_other_msids_of_same_song():
    before=dataset();after=deepcopy(before)
    after['labels'][0]['song_name']='Corrected'
    result=affected_periods(before,after,policy_before='v1',policy_after='v1')
    assert result['months']==['2026-01','2026-02']
    assert result['changed_song_keys']==['r']


def test_global_artist_name_change_affects_other_songs_and_periods():
    before=dataset();after=deepcopy(before)
    after['credits'].append({'song_key':'unknown','artist_mbid':'main','artist_name':'000 Corrected'})
    result=affected_periods(before,after,policy_before='v1',policy_after='v1')
    assert result['months']==['2026-01','2026-02']
    assert result['changed_artists']==['main']


def test_policy_change_requires_full_rebuild_and_expands_all_periods():
    before=dataset()
    result=affected_periods(before,deepcopy(before),policy_before='v1',policy_after='v2')
    assert result['requires_full_rebuild'] is True
    assert result['months']==['2026-01','2026-02']


@pytest.mark.parametrize('fault',['duplicate','bad_month','negative','missing_reference'])
def test_invalid_snapshots_are_rejected_instead_of_underestimating_periods(fault):
    before=dataset();after=deepcopy(before)
    if fault=='duplicate':after['monthly'].append(deepcopy(after['monthly'][0]))
    elif fault=='bad_month':after['monthly'][0]['listened_month']='2026-13'
    elif fault=='negative':after['monthly'][0]['play_count']=-1
    else:after['dimension'].pop(0)
    with pytest.raises(ValueError):affected_periods(before,after,policy_before='v1',policy_after='v1')


def test_cdf_updates_and_deletes_cover_before_and_after_months():
    changes=[{'listened_month':month,'_change_type':kind,'_commit_version':1} for month,kind in
             [('2026-12','update_preimage'),('2027-01','update_postimage'),('2025-01','delete')]]
    result=cdf_periods(changes,source_id='same',previous_source_id='same',previous_version=0,ending_version=1,range_complete=True)
    assert result=={'months':['2025-01','2026-12','2027-01'],'years':['2025','2026','2027']}


@pytest.mark.parametrize('kwargs',[
    {'source_id':'replaced'},{'range_complete':False},{'ending_version':0},{'previous_version':2}])
def test_incomplete_or_replaced_cdf_is_refused(kwargs):
    options=dict(source_id='same',previous_source_id='same',previous_version=0,ending_version=1,range_complete=True)
    options.update(kwargs)
    with pytest.raises(ValueError):cdf_periods([{'listened_month':'2026-01','_change_type':'insert','_commit_version':1}],**options)


@pytest.mark.parametrize('kind,month',[('update_preimage','2026-01'),('unknown','2026-01'),('insert','2026-13')])
def test_unpaired_updates_or_unknown_actions_and_bad_months_are_refused(kind,month):
    with pytest.raises(ValueError):
        cdf_periods([{'listened_month':month,'_change_type':kind,'_commit_version':1}],source_id='same',
                    previous_source_id='same',previous_version=0,ending_version=1,range_complete=True)


def test_cdf_replay_without_a_new_version_has_no_affected_periods():
    assert cdf_periods([],source_id='same',previous_source_id='same',previous_version=1,ending_version=1,
                       range_complete=True)=={'months':[],'years':[]}
