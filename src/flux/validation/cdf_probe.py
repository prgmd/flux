import json
from collections import Counter,defaultdict
from datetime import datetime,timezone
from pathlib import Path

from flux.silver import dedup,mbid
from flux.silver.quality import judge


def expected(raw):
    groups=defaultdict(list);quarantine=[]
    for row in raw:
        doc=json.loads(row['raw_json']);at=row['_ingested_at']
        if isinstance(at,str):at=datetime.fromisoformat(at)
        verdict=judge(doc,ingested_at=at.replace(tzinfo=timezone.utc).timestamp())
        if not verdict.ok:
            quarantine.append((row['_source_dump'],row['_record_id'],verdict.reason,row['raw_json']));continue
        key=(row['user_name'],row['listened_at'],doc.get('recording_msid'))
        if not isinstance(key[2],str) or not key[2].strip():raise ValueError('missing MSID')
        resolved=mbid.resolve(doc)
        groups[key].append((dedup.Candidate(row['_record_id'],row['_source_dump'],resolved.source,resolved.artist_mbids),doc,resolved))
    counts=Counter();metadata=set()
    for (user,stamp,msid),items in groups.items():
        winner=dedup.pick(item[0] for item in items)
        _,doc,resolved=next(item for item in items if item[0] is winner)
        month=datetime.fromtimestamp(stamp,timezone.utc).strftime('%Y-%m')
        counts[user,msid,month]+=1;meta=doc['track_metadata']
        metadata.add((msid,meta.get('track_name'),meta.get('artist_name'),resolved.recording_mbid,
                      resolved.artist_mbids or None,resolved.source.value))
    return {'counts':dict(counts),'quarantine':quarantine,'metadata':list(metadata)}


def compare(wanted,monthly,quarantine,metadata):
    actual={};invalid=0
    for row in monthly:
        key=(row['user_name'],row['recording_msid'],row['listened_month']);value=row['play_count']
        invalid+=key in actual or type(value) is not int or value<=0
        actual[key]=value
    if invalid or actual!=wanted['counts']:raise ValueError('monthly key/value mismatch')
    if Counter(quarantine)!=Counter(wanted['quarantine']):raise ValueError('quarantine mismatch')
    if len(set(metadata))!=len(metadata) or not set(wanted['metadata'])<=set(metadata):raise ValueError('metadata mismatch')
    return {'monthly_rows':len(actual),'plays':sum(actual.values()),'quarantine_rows':len(quarantine),
            'current_metadata_variants':len(wanted['metadata']),'historical_variants':len(set(metadata)-set(wanted['metadata'])),
            'monthly_mismatches':0,'quarantine_mismatches':0,'missing_current_metadata':0}


def release_interrupted(root,*,confirmed_exit_code,input_sha256):
    root=Path(root).resolve();marker=json.loads((root/'interrupted.json').read_text(encoding='utf-8'))
    lock=root/'silver/.writer.lock'
    if (confirmed_exit_code!=86 or marker['expected_exit_code']!=86 or marker['input_sha256']!=input_sha256
        or lock.read_text()!=str(marker['pid'])):raise ValueError('interrupted writer must be confirmed and match experiment')
    lock.unlink()
    return marker
