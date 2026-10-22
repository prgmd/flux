import hashlib
import json
import os
import re
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime,timezone
from pathlib import Path

from flux.silver.recording_link_policy import POLICY_VERSION,RecordingEvidence,assess_pair


OBSERVATION_FIELDS=('recording_msid','artist_name','track_name','recording_mbid','artist_mbids','mbid_source')
STATUSES={'approved','keep_separate','hold'}


class ReviewBusy(RuntimeError):
    pass


class JournalCorrupt(ValueError):
    pass


def _json(value):
    return json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)


def _digest(prefix,value):
    return prefix+':'+hashlib.sha256(_json(value).encode('utf-8')).hexdigest()


def observation_id(original):
    return _digest('observation',{key:original.get(key) for key in OBSERVATION_FIELDS})


def make_candidate(observations,facts,source_state,proofs):
    if len(observations)!=2 or len(facts)!=2:
        raise ValueError('two observations and facts required')
    if not all(key in source_state for key in ('silver_fingerprint','official_snapshot','sample_sha256')):
        raise ValueError('source state incomplete')
    members=[]
    for original,fact in zip(observations,facts):
        if (original.get('recording_msid'),original.get('artist_name'),original.get('track_name'),original.get('recording_mbid'))!=(fact.source_msid,fact.artist_name,fact.track_name,fact.observed_recording_mbid):
            raise ValueError('facts must preserve original observation')
        if fact.snapshot_id is not None and fact.snapshot_id!=source_state['official_snapshot']:
            raise ValueError('snapshot mismatch')
        members.append((observation_id(original),original,asdict(fact)))
    members.sort(key=lambda row:row[0])
    ids=[row[0] for row in members]
    base={'schema_version':1,'record_type':'candidate','rule_version':POLICY_VERSION,
          'candidate_id':_digest('candidate',ids),'observation_ids':ids,
          'source_state':source_state,'observations':[row[1] for row in members],
          'facts':[row[2] for row in members],'proofs':proofs,
          'assessment':assess_pair(*(RecordingEvidence(**row[2]) for row in members))}
    base=json.loads(_json(base))
    return {**base,'evidence_id':_digest('evidence',base)}


def _validate_candidate(record):
    base={key:value for key,value in record.items() if key!='evidence_id'}
    if record['schema_version']!=1 or record['record_type']!='candidate' or record['evidence_id']!=_digest('evidence',base):
        raise ValueError('candidate digest or schema mismatch')
    ids=[observation_id(row) for row in record['observations']]
    if ids!=sorted(ids) or ids!=record['observation_ids'] or record['candidate_id']!=_digest('candidate',ids):
        raise ValueError('candidate identity mismatch')
    if record['rule_version']==POLICY_VERSION:
        rebuilt=make_candidate(record['observations'],[RecordingEvidence(**r) for r in record['facts']],record['source_state'],record['proofs'])
        if record!=rebuilt:
            raise ValueError('candidate evidence mismatch')


def _read_lines(path):
    if not path.exists():
        return []
    raw=path.read_bytes()
    if raw and not raw.endswith(b'\n'):
        raise JournalCorrupt('unterminated journal line: '+path.name)
    try:
        return [json.loads(line) for line in raw.decode('utf-8').splitlines()]
    except (ValueError,UnicodeError) as error:
        raise JournalCorrupt('invalid journal: '+path.name) from error


def _validate_decision(record,candidate):
    base={key:value for key,value in record.items() if key!='decision_id'}
    if record['schema_version']!=1 or record['record_type']!='decision' or record['decision_id']!=_digest('decision',base):
        raise ValueError('decision digest or schema mismatch')
    if record['candidate_id']!=candidate['candidate_id'] or record['rule_version']!=candidate['rule_version']:
        raise ValueError('decision target mismatch')
    if record['status'] not in STATUSES or record['reviewer_kind'] not in ('human','agent'):
        raise ValueError('unsupported decision or reviewer')
    if not all(isinstance(record[key],str) and record[key].strip() for key in ('reviewer_id','reason_code','note')):
        raise ValueError('reviewer and reason required')
    if not re.fullmatch('[a-z][a-z0-9_]*',record['reason_code']):
        raise ValueError('invalid reason code')
    at=datetime.fromisoformat(record['reviewed_at'].replace('Z','+00:00'))
    if at.tzinfo is None or at.utcoffset()!=timezone.utc.utcoffset(at):
        raise ValueError('UTC review timestamp required')
    expected={'approved':'eligible','keep_separate':'keep_separate'}
    if record['status'] in expected and candidate['assessment']['status']!=expected[record['status']]:
        raise ValueError('approved requires eligible; separation requires confirmed conflict')


class ReviewJournal:
    def __init__(self,path):
        self.path=Path(path)
        self.path.mkdir(parents=True,exist_ok=True)

    @contextmanager
    def _lock(self):
        lock=self.path/'.lock'
        try:
            lock.mkdir()
        except FileExistsError as error:
            raise ReviewBusy('review journal busy') from error
        try:
            yield
        finally:
            lock.rmdir()

    def _state(self):
        evidence={};candidates={};decisions={};ids=set()
        try:
            for record in _read_lines(self.path/'candidates.jsonl'):
                _validate_candidate(record)
                if record['evidence_id'] in evidence:
                    raise ValueError('duplicate evidence')
                evidence[record['evidence_id']]=record
                candidates[record['candidate_id']]=record
            for record in _read_lines(self.path/'decisions.jsonl'):
                target=evidence[record['evidence_id']]
                _validate_decision(record,target)
                previous=decisions.get(record['candidate_id'])
                if record['supersedes']!=(previous['decision_id'] if previous else None) or record['decision_id'] in ids:
                    raise ValueError('broken decision history')
                ids.add(record['decision_id'])
                decisions[record['candidate_id']]=record
        except (KeyError,TypeError,ValueError) as error:
            raise JournalCorrupt(str(error)) from error
        return evidence,candidates,decisions

    def _append(self,name,record):
        raw=(_json(record)+'\n').encode('utf-8')
        with (self.path/name).open('ab') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

    def register(self,record):
        _validate_candidate(record)
        with self._lock():
            evidence,_,_=self._state()
            if record['evidence_id'] not in evidence:
                self._append('candidates.jsonl',record)
        return record['evidence_id']

    def decide(self,evidence_id,*,status,reason_code,note,reviewer_kind,reviewer_id,supersedes=None,reviewed_at=None):
        with self._lock():
            evidence,candidates,decisions=self._state()
            if evidence_id not in evidence:
                raise ValueError('unknown evidence')
            candidate=evidence[evidence_id]
            if candidates[candidate['candidate_id']]['evidence_id']!=evidence_id or candidate['rule_version']!=POLICY_VERSION:
                raise ValueError('stale evidence or rule')
            previous=decisions.get(candidate['candidate_id'])
            if supersedes!=(previous['decision_id'] if previous else None):
                raise ValueError('latest predecessor required')
            at=reviewed_at or datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
            base={'schema_version':1,'record_type':'decision','candidate_id':candidate['candidate_id'],
                  'evidence_id':evidence_id,'rule_version':candidate['rule_version'],'status':status,
                  'reason_code':reason_code,'note':note,'reviewer_kind':reviewer_kind,'reviewer_id':reviewer_id,
                  'reviewed_at':at,'supersedes':supersedes}
            record={**base,'decision_id':_digest('decision',base)}
            _validate_decision(record,candidate)
            self._append('decisions.jsonl',record)
            return record

    def export_approved(self,source_state,*,mode='production'):
        if mode not in ('production','sandbox'):
            raise ValueError('unsupported export mode')
        with self._lock():
            _,candidates,decisions=self._state()
        mappings={};excluded=[];accepted=human=0
        for cid,candidate in sorted(candidates.items()):
            decision=decisions.get(cid)
            reason=None
            if candidate['source_state']!=source_state or candidate['rule_version']!=POLICY_VERSION:
                reason='stale_source_or_rule'
            elif decision is None or decision['evidence_id']!=candidate['evidence_id']:
                reason='review_required'
            elif decision['status']!='approved':
                reason=decision['status']
            elif mode=='production' and decision['reviewer_kind']!='human':
                reason='human_approval_required'
            if reason:
                excluded.append({'candidate_id':cid,'reason':reason})
                continue
            accepted+=1;human+=decision['reviewer_kind']=='human'
            for oid,fact in zip(candidate['observation_ids'],candidate['facts']):
                target=(fact['resolved_recording_mbid'],tuple(sorted(fact['official_artist_mbids'])),fact['version_key'])
                if oid in mappings:
                    if mappings[oid]['target']!=target:
                        raise ValueError('contradictory approved observation targets')
                    mappings[oid]['decision_ids'].append(decision['decision_id'])
                    continue
                mappings[oid]={'observation_id':oid,'source_msid':fact['source_msid'],
                    'recording_mbid':target[0],'official_artist_mbids':list(target[1]),'version_key':target[2],
                    'candidate_id':cid,'evidence_id':candidate['evidence_id'],'decision_ids':[decision['decision_id']],
                    'reviewer_kind':decision['reviewer_kind'],'target':target}
        output=[{key:value for key,value in mappings[oid].items() if key!='target'} for oid in sorted(mappings)]
        return {'mode':mode,'approved_candidates':accepted,'human_approved_candidates':human,'mappings':output,'excluded':excluded}
