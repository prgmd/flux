from dataclasses import dataclass
from uuid import UUID

from flux.silver.name_candidates import normalize_text


RULE_ID = 'R9'
POLICY_VERSION = 'official-id-accent-link-v1'


@dataclass(frozen=True)
class RecordingEvidence:
    source_msid: str
    artist_name: str
    track_name: str
    official_artist_mbids: tuple[str,...] = ()
    observed_recording_mbid: str | None = None
    resolved_recording_mbid: str | None = None
    official_artist_verified: bool = False
    official_recording_verified: bool = False
    version_key: str | None = None
    version_verified: bool = False
    snapshot_id: str | None = None
    evidence_ref: str | None = None
    conflict: bool = False


def _present(value):
    return isinstance(value,str) and bool(value.strip())


def _mbid(value):
    if not _present(value):
        return None
    try:
        return str(UUID(value))
    except ValueError:
        return None


def _artists(values):
    if not isinstance(values,(tuple,list)) or not values:
        return None
    parsed={_mbid(v) for v in values}
    return parsed if None not in parsed else None


def assess_pair(left,right):
    def decision(status,reason):
        return {'rule_id':RULE_ID,'policy_version':POLICY_VERSION,'status':status,'reason':reason}
    if not all(_present(r.source_msid) for r in (left,right)):
        return decision('hold','missing_source')
    if left.source_msid==right.source_msid:
        return decision('not_candidate','same_source')
    try:
        keys=[(normalize_text(r.artist_name),normalize_text(r.track_name)) for r in (left,right)]
    except ValueError:
        return decision('hold','missing_name')
    if keys[0]!=keys[1]:
        return decision('not_candidate','different_comparison_key')
    if any(r.conflict is not False for r in (left,right)):
        return decision('hold','conflicting_evidence')
    artists=[_artists(r.official_artist_mbids) for r in (left,right)]
    recordings=[_mbid(r.resolved_recording_mbid) for r in (left,right)]
    if (any(ids is None for ids in artists) or any(gid is None for gid in recordings)
            or any(_mbid(r.observed_recording_mbid) is None for r in (left,right))):
        return decision('hold','missing_or_invalid_official_id')
    if not all(r.official_artist_verified is True and r.official_recording_verified is True
               and _present(r.evidence_ref) and _present(r.snapshot_id) for r in (left,right)):
        return decision('hold','official_evidence_unverified')
    if left.snapshot_id!=right.snapshot_id:
        return decision('hold','different_official_snapshot')
    if not all(r.version_verified is True and _present(r.version_key) for r in (left,right)):
        return decision('hold','version_unverified')
    if artists[0]!=artists[1]:
        return decision('keep_separate','artist_id_conflict')
    if recordings[0]!=recordings[1]:
        return decision('keep_separate','recording_id_conflict')
    if left.version_key!=right.version_key:
        return decision('keep_separate','version_conflict')
    return decision('eligible','same_verified_artist_recording_and_version')
