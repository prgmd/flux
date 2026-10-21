from collections import defaultdict
from itertools import combinations,product

from flux.silver.name_candidates import NORMALIZATION_STEPS,normalize_text


def previous_text(value):
    if not isinstance(value,str) or not value.strip():
        raise ValueError('name must be a nonempty string')
    for name,transform in NORMALIZATION_STEPS:
        if name!='latin_accents':
            value=transform(value)
    return value


def previous_key(row):
    return (previous_text(row['artist_name']),previous_text(row['track_name']))


def comparison_key(row):
    return (normalize_text(row['artist_name']),normalize_text(row['track_name']))


def analyze_accent_group(rows):
    keys={comparison_key(r) for r in rows}
    if len(keys)!=1:
        raise ValueError('one comparison key required')
    buckets=defaultdict(list)
    for row in rows:
        buckets[previous_key(row)].append(row)
    if len(buckets)<2:
        raise ValueError('at least two previous keys required')
    references={key:{r['resolved_mbid'] for r in members if r.get('resolved_mbid')} for key,members in buckets.items()}
    conflicts=set();shared=set()
    for left,right in combinations(references,2):
        shared.update(references[left]&references[right])
        conflicts.update(tuple(sorted((a,b))) for a,b in product(references[left],references[right]) if a!=b)
    refs=set().union(*references.values())
    unknown=sum(not r.get('resolved_mbid') for r in rows)
    verdict=('new_reference_conflict' if conflicts else 'existing_reference_conflict' if len(refs)>1 else
             'reference_missing' if not refs else 'same_reference_with_unverified' if unknown else 'same_reference_candidate')
    return {'key':list(next(iter(keys))),'rows':rows,'previous_key_count':len(buckets),
            'previous_buckets':[{'key':list(key),'reference_mbids':sorted(references[key]),
                                 'unknown_rows':sum(not r.get('resolved_mbid') for r in members)}
                                for key,members in sorted(buckets.items())],
            'known_reference_mbids':sorted(refs),'unknown_reference_rows':unknown,
            'new_conflicting_reference_pairs':[list(p) for p in sorted(conflicts)],
            'shared_reference_mbids':sorted(shared),'verdict':verdict,
            'artist_accent_changed_rows':sum(previous_text(r['artist_name'])!=normalize_text(r['artist_name']) for r in rows),
            'title_accent_changed_rows':sum(previous_text(r['track_name'])!=normalize_text(r['track_name']) for r in rows)}


def accent_groups(rows):
    groups=defaultdict(list)
    for row in rows:
        groups[comparison_key(row)].append(row)
    return [analyze_accent_group(members) for _,members in sorted(groups.items())
            if len({previous_key(r) for r in members})>1]


def representative_rows(rows):
    selected={}
    for row in sorted(rows,key=lambda r:(r['recording_msid'],r.get('recording_mbid') or '',r['artist_name'],r['track_name'])):
        selected.setdefault((previous_key(row),row.get('resolved_mbid')),row)
    return list(selected.values())
