import json
from datetime import datetime,timezone

import pytest

from flux.validation.cdf_probe import compare,expected,release_interrupted


def raw(number,*,user='u',msid='song',stamp=1769903999,played=None,dump='a'):
    document={'user_name':user,'timestamp':stamp,'recording_msid':msid,
              'track_metadata':{'track_name':'Song','artist_name':'Artist','additional_info':{'ms_played':played}}}
    return {'_record_id':number,'_source_dump':dump,'user_name':user,'listened_at':stamp,
            '_ingested_at':datetime(2026,10,22,tzinfo=timezone.utc),'raw_json':json.dumps(document)}


def test_python_expectation_filters_before_dedup_and_retains_repeated_listens():
    result=expected([raw(1),raw(2,dump='b',played=29999),raw(3,stamp=1769904000),raw(4,stamp=1769904001),raw(5,dump='c')])
    assert result['counts']=={('u','song','2026-01'):1,('u','song','2026-02'):2}
    assert len(result['quarantine'])==1


def test_comparison_finds_wrong_keys_even_if_total_is_equal():
    wanted=expected([raw(1),raw(2,msid='other')])
    actual=[{'user_name':'u','recording_msid':'song','listened_month':'2026-01','play_count':2}]
    with pytest.raises(ValueError,match='monthly'):compare(wanted,actual,wanted['quarantine'],wanted['metadata'])


def test_historical_metadata_is_allowed_but_missing_current_variant_is_not():
    wanted=expected([raw(1)])
    actual=[{'user_name':'u','recording_msid':'song','listened_month':'2026-01','play_count':1}]
    assert compare(wanted,actual,wanted['quarantine'],wanted['metadata']+[('historic',)])['historical_variants']==1
    with pytest.raises(ValueError,match='metadata'):compare(wanted,actual,wanted['quarantine'],[])


def test_stale_lock_release_requires_matching_exit_marker_and_explicit_confirmation(tmp_path):
    root=tmp_path/'probe';(root/'silver').mkdir(parents=True)
    lock=root/'silver/.writer.lock';lock.write_text('123')
    (root/'interrupted.json').write_text(json.dumps({'pid':123,'expected_exit_code':86,'input_sha256':'fixed'}))
    with pytest.raises(ValueError):release_interrupted(root,confirmed_exit_code=None,input_sha256='fixed')
    with pytest.raises(ValueError):release_interrupted(root,confirmed_exit_code=86,input_sha256='changed')
    lock.write_text('124')
    with pytest.raises(ValueError):release_interrupted(root,confirmed_exit_code=86,input_sha256='fixed')
    assert lock.exists()
    lock.write_text('123')
    release_interrupted(root,confirmed_exit_code=86,input_sha256='fixed')
    assert not lock.exists()
