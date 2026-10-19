import argparse
import hashlib
import json
import tarfile
import time
from pathlib import Path

TABLES = {'artist', 'artist_credit', 'artist_credit_name', 'recording'}


def extract(archive, output, tables=None):
    tables = TABLES if tables is None else set(tables)
    if not tables or not tables <= TABLES | {'recording_gid_redirect'}:
        raise ValueError('unsupported tables')
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    before = archive.stat()
    extracted = {}
    with tarfile.open(archive, 'r|bz2') as tar:
        for member in tar:
            if not member.isfile() or member.name not in {'mbdump/'+name for name in tables}:
                continue
            name = member.name.split('/')[1]
            if name in extracted:
                raise ValueError('duplicate table')
            target = output/name
            digest = hashlib.sha256()
            size = 0
            with tar.extractfile(member) as src, target.with_suffix('.part').open('xb') as dst:
                while chunk := src.read(8*1024*1024):
                    dst.write(chunk)
                    digest.update(chunk)
                    size += len(chunk)
            if size != member.size:
                raise ValueError('truncated table')
            target.with_suffix('.part').replace(target)
            extracted[name] = {'bytes':size,'sha256':digest.hexdigest()}
            print(name,size,round(time.monotonic()-started,2),flush=True)
            if set(extracted)==tables:
                break
    if set(extracted)!=tables:
        raise ValueError('missing tables: '+str(tables-set(extracted)))
    after = archive.stat()
    if (before.st_size,before.st_mtime_ns)!=(after.st_size,after.st_mtime_ns):
        raise ValueError('archive changed')
    result = {'completed':True,'archive':str(archive.resolve()),'tables':extracted,
              'seconds':round(time.monotonic()-started,2),'source_release':archive.parent.name}
    (output/'result.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    return result


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--archive',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--tables',nargs='+',choices=sorted(TABLES | {'recording_gid_redirect'}))
    a=p.parse_args()
    extract(a.archive,a.output,a.tables)
