import json
import tarfile
from datetime import datetime,timezone
from pathlib import Path


def timestamp(value):
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('dump timestamp needs timezone')
    return result.astimezone(timezone.utc)


def dump_header(entry):
    from compression import zstd

    path=Path(entry['path']);stat=path.stat()
    if stat.st_size!=entry['size']:raise ValueError('dump size mismatch')
    result={'dump':entry['dump'],'path':str(path),'bytes':stat.st_size,'mtime_ns':stat.st_mtime_ns,
            'recorded_checksums':entry.get('checksums',{}),'archive_rehashed':False,'has_listen_record':False}
    with zstd.ZstdFile(path) as stream,tarfile.open(fileobj=stream,mode='r|') as archive:
        for number,member in enumerate(archive):
            if number>32:raise ValueError('dump header not found before payload')
            name=member.name.rsplit('/',1)[-1]
            if name in ('START_TIMESTAMP','END_TIMESTAMP'):
                value=archive.extractfile(member).read(1024).decode().strip()
                result['start' if name=='START_TIMESTAMP' else 'end']=timestamp(value).isoformat()
            if name.endswith('.listens'):
                first=archive.extractfile(member).readline(1024*1024)
                if first.strip():
                    value=json.loads(first)
                    result['has_listen_record']=isinstance(value,dict)
                break
    if 'start' not in result or 'end' not in result:raise ValueError('missing dump timestamps')
    return result


def summarize_windows(records):
    accepted=[];excluded=[]
    for row in records:
        start,end=timestamp(row['start']),timestamp(row['end'])
        if end<start:raise ValueError('reversed dump window')
        if not row['has_listen_record'] or start==end:excluded.append(row['dump'])
        else:accepted.append((start,end,row['dump']))
    accepted.sort();gaps=[];overlaps=[];chains=[];chain=[];previous=None
    for start,end,name in accepted:
        if previous is not None and start!=previous:
            target=gaps if start>previous else overlaps
            target.append({'previous_end':previous.isoformat(),'next_start':start.isoformat(),'next_dump':name})
            chains.append(chain);chain=[]
        chain.append(name);previous=max(previous,end) if previous else end
    if chain:chains.append(chain)
    return {'accepted':len(accepted),'excluded':excluded,'gaps':gaps,'overlaps':overlaps,
            'continuous':bool(accepted) and not gaps and not overlaps,'chains':chains,
            'longest_contiguous_dumps':max(map(len,chains),default=0),
            'collection_route':'scripts/pipeline/fetch_incremental_range.py',
            'new_input_sequence_ready':False,
            'reason':'local inventory only; future inputs must be captured and checked before P2'}
