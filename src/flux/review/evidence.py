import re

from flux.gold.monthly_marts import VARIANTS
from flux.silver.accent_review import previous_text
from flux.silver.name_candidates import artist_alias_key,normalize_text
from flux.silver.recording_link_policy import RecordingEvidence


def _labels(title,comment=''):
    qualifiers=' '.join(re.findall(r'\([^()]*\)|\[[^\[\]]*\]| - .*$',title or ''))
    text=(qualifiers+' '+comment).lower()
    labels={name for name,pattern in VARIANTS.items() if re.search(pattern,text)}
    if re.search(r'dj\W*mix',text):
        labels.add('dj_mix')
    if re.search(r'dolby\s+atmos',text):
        labels.add('atmos')
    return labels


def build_evidence(original,official,credits,*,snapshot_id,evidence_ref,redirect=None,conflict=False):
    official=official or {}
    original_mbid=original.get('recording_mbid')
    resolved=official.get('gid') if original_mbid else None
    direct=bool(original_mbid and original_mbid==resolved)
    redirected=bool(original_mbid and redirect and redirect.get('gid')==original_mbid
                    and str(redirect.get('new_id'))==str(official.get('id')))
    recording_verified=bool(resolved and (direct or redirected))
    artists=tuple(sorted({row['artist_mbid'] for row in credits}))
    local_artist=original['artist_name']
    exact=any(previous_text(local_artist)==previous_text(row['name']) for row in credits)
    alias=artist_alias_key(local_artist)
    alias_verified=bool(alias and alias in artists)
    submitted=set(original.get('artist_mbids') or [])
    corroborated=bool(artists and submitted==set(artists) and any(
        normalize_text(local_artist)==normalize_text(row['name']) for row in credits))
    artist_verified=bool(recording_verified and (exact or alias_verified or corroborated))
    artist_check=('official_name' if exact else 'reviewed_alias' if alias_verified else
                  'submitted_ids_and_credit' if corroborated else 'unverified')
    title_equal=bool(resolved and normalize_text(original['track_name'])==normalize_text(official['name']))
    official_labels=_labels(official.get('name'),official.get('comment') or '')
    local_labels=_labels(original['track_name'])
    comment=official.get('comment') or ''
    understood=not comment or bool(official_labels) or bool(re.search(
        r'\b(original mix|album version|lp version|single version|studio)\b',comment.lower()))
    nonvideo=official.get('video') is False or official.get('video') in ('f','false')
    version_verified=bool(recording_verified and title_equal and nonvideo and understood
                          and local_labels.issubset(official_labels))
    version_key=(('+'.join(sorted(official_labels)) or 'recording')+':'+resolved) if version_verified else None
    fact=RecordingEvidence(source_msid=original['recording_msid'],artist_name=local_artist,
        track_name=original['track_name'],official_artist_mbids=artists,observed_recording_mbid=original_mbid,
        resolved_recording_mbid=resolved,official_artist_verified=artist_verified,
        official_recording_verified=recording_verified,version_key=version_key,version_verified=version_verified,
        snapshot_id=snapshot_id,evidence_ref=evidence_ref,conflict=conflict)
    proof={'official_recording':official,'official_credits':credits,'redirect':redirect,
           'recording_check':'direct' if direct else 'official_redirect' if redirected else 'unverified',
           'artist_check':artist_check,'artist_verified':artist_verified,'title_comparison_equal':title_equal,
           'official_version_labels':sorted(official_labels),'local_version_labels':sorted(local_labels),
           'version_verified':version_verified,'unmarked_is_not_original':True,'scope_conflict':conflict}
    return fact,proof
