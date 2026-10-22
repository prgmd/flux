import json
from dataclasses import replace

import pytest

from flux.review.journal import ReviewJournal,ReviewBusy,JournalCorrupt,make_candidate
from flux.silver.recording_link_policy import RecordingEvidence


A='00000000-0000-0000-0000-000000000001'
R='00000000-0000-0000-0000-000000000002'
SOURCE={'silver_fingerprint':{'metadata':'one'},'official_snapshot':'test-snapshot','sample_sha256':'sample-one'}


def candidate(*,missing=False,source=None,other='second'):
    originals=[{'recording_msid':'first','artist_name':'Artíst','track_name':'Café','recording_mbid':R},
               {'recording_msid':other,'artist_name':'Artist','track_name':'Cafe','recording_mbid':None if missing else R}]
    facts=[RecordingEvidence(source_msid=o['recording_msid'],artist_name=o['artist_name'],track_name=o['track_name'],
        official_artist_mbids=(A,),observed_recording_mbid=o['recording_mbid'],resolved_recording_mbid=o['recording_mbid'],
        official_artist_verified=True,official_recording_verified=not (missing and i==1),
        version_key='recording:'+R,version_verified=True,snapshot_id='test-snapshot',evidence_ref='unit-test:'+str(i))
        for i,o in enumerate(originals)]
    return make_candidate(originals,facts,source or SOURCE,{'kind':'synthetic unit-test evidence'})


def decide(journal,record,status='approved',**changes):
    args={'status':status,'reason_code':'verified_same_recording','note':'Synthetic review only',
          'reviewer_kind':'human','reviewer_id':'unit-test-reviewer'}
    args.update(changes)
    return journal.decide(record['evidence_id'],**args)


def test_candidate_identity_is_order_independent_and_original_is_preserved(tmp_path):
    record=candidate()
    reverse=make_candidate(list(reversed(record['observations'])),
        [RecordingEvidence(**f) for f in reversed(record['facts'])],SOURCE,record['proofs'])
    assert record==reverse
    journal=ReviewJournal(tmp_path/'review')
    assert journal.register(record)==record['evidence_id']
    assert journal.register(record)==record['evidence_id']
    saved=[json.loads(line) for line in (tmp_path/'review/candidates.jsonl').read_text(encoding='utf-8').splitlines()]
    assert saved==[record]
    assert {o['artist_name'] for o in saved[0]['observations']}=={'Artíst','Artist'}


def test_decision_revisions_are_append_only_and_require_latest_predecessor(tmp_path):
    journal=ReviewJournal(tmp_path/'review');record=candidate();journal.register(record)
    first=decide(journal,record)
    with pytest.raises(ValueError,match='predecessor'):
        decide(journal,record,status='hold')
    second=decide(journal,record,status='hold',supersedes=first['decision_id'])
    assert first['decision_id']==second['supersedes']
    assert journal.export_approved(SOURCE)['mappings']==[]
    lines=(tmp_path/'review/decisions.jsonl').read_text(encoding='utf-8').splitlines()
    assert [json.loads(line)['status'] for line in lines]==['approved','hold']


def test_missing_evidence_never_approves_and_agent_is_not_a_human_approval(tmp_path):
    journal=ReviewJournal(tmp_path/'review');missing=candidate(missing=True);journal.register(missing)
    with pytest.raises(ValueError,match='eligible'):decide(journal,missing)
    decide(journal,missing,status='hold')
    record=candidate();journal.register(record)
    decide(journal,record,reviewer_kind='agent',reviewer_id='test-agent')
    assert journal.export_approved(SOURCE)['mappings']==[]
    sandbox=journal.export_approved(SOURCE,mode='sandbox')
    assert len(sandbox['mappings'])==2 and sandbox['human_approved_candidates']==0
    assert {m['reviewer_kind'] for m in sandbox['mappings']}=={'agent'}


def test_new_evidence_and_source_changes_invalidate_old_approval(tmp_path):
    journal=ReviewJournal(tmp_path/'review');record=candidate();journal.register(record)
    first=decide(journal,record)
    changed={**SOURCE,'sample_sha256':'sample-two'}
    updated=candidate(source=changed)
    assert updated['candidate_id']==record['candidate_id'] and updated['evidence_id']!=record['evidence_id']
    journal.register(updated)
    assert journal.export_approved(changed)['mappings']==[]
    with pytest.raises(ValueError,match='stale'):decide(journal,record,supersedes=first['decision_id'])
    decide(journal,updated,supersedes=first['decision_id'])
    assert len(journal.export_approved(changed)['mappings'])==2
    assert journal.export_approved(SOURCE)['mappings']==[]


def test_corruption_and_incomplete_tail_are_not_silently_ignored(tmp_path):
    journal=ReviewJournal(tmp_path/'review');record=candidate();journal.register(record)
    path=tmp_path/'review/candidates.jsonl'
    original=path.read_bytes()
    path.write_bytes(original+b'{"partial":')
    with pytest.raises(JournalCorrupt):decide(journal,record)
    assert path.read_bytes()==original+b'{"partial":'
    path.write_bytes(original)
    altered=json.loads(original);altered['assessment']['status']='hold'
    path.write_text(json.dumps(altered)+'\n',encoding='utf-8')
    with pytest.raises(JournalCorrupt):journal.export_approved(SOURCE)


def test_busy_writer_and_false_provenance_are_rejected(tmp_path):
    journal=ReviewJournal(tmp_path/'review');record=candidate()
    (tmp_path/'review/.lock').mkdir()
    with pytest.raises(ReviewBusy):journal.register(record)
    (tmp_path/'review/.lock').rmdir()
    facts=[RecordingEvidence(**f) for f in record['facts']]
    facts[0]=replace(facts[0],observed_recording_mbid=None)
    with pytest.raises(ValueError,match='original'):make_candidate(record['observations'],facts,SOURCE,{})
    journal.register(record)
    with pytest.raises(ValueError):decide(journal,record,status='keep_separate')


def test_unknown_decision_target_and_broken_history_are_rejected(tmp_path):
    journal=ReviewJournal(tmp_path/'review')
    with pytest.raises(ValueError):journal.decide('absent',status='hold',reason_code='unknown',note='No evidence',reviewer_kind='agent',reviewer_id='test')
    record=candidate();journal.register(record);decide(journal,record)
    path=tmp_path/'review/decisions.jsonl'
    decision=json.loads(path.read_text(encoding='utf-8'));decision['supersedes']='absent'
    path.write_text(json.dumps(decision)+'\n',encoding='utf-8')
    with pytest.raises(JournalCorrupt):journal.export_approved(SOURCE)


def test_rule_change_requires_revalidation_even_with_a_previous_human_approval(tmp_path,monkeypatch):
    import flux.review.journal as module

    journal=ReviewJournal(tmp_path/'review');record=candidate();journal.register(record);decide(journal,record)
    monkeypatch.setattr(module,'POLICY_VERSION','next-policy')
    assert journal.export_approved(SOURCE)['mappings']==[]
    with pytest.raises(ValueError,match='stale'):decide(journal,record)


def test_contradictory_approved_targets_for_one_observation_fail_closed(tmp_path):
    journal=ReviewJournal(tmp_path/'review');first=candidate();journal.register(first);decide(journal,first)
    other=candidate(other='third')
    alternate='00000000-0000-0000-0000-000000000003'
    facts=[replace(RecordingEvidence(**r),resolved_recording_mbid=alternate,version_key='recording:'+alternate) for r in other['facts']]
    changed=make_candidate(other['observations'],facts,SOURCE,{'kind':'contradictory synthetic evidence'})
    journal.register(changed);decide(journal,changed)
    with pytest.raises(ValueError,match='contradictory'):journal.export_approved(SOURCE)
