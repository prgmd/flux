import hashlib
import json
from pathlib import Path
from urllib.parse import unquote


def sha256(path):
    value=hashlib.sha256()
    with Path(path).open('rb') as stream:
        while chunk:=stream.read(8*1024*1024):value.update(chunk)
    return value.hexdigest()


def pin_delta(root,version=None):
    root=Path(root).resolve();log=root/'_delta_log'
    commits=sorted(p for p in log.glob('*.json') if len(p.stem)==20 and p.stem.isdigit())
    if not commits:raise ValueError('no Delta JSON history')
    latest=int(commits[-1].stem)
    version=latest if version is None else version
    if type(version) is not int or version<0 or version>latest:raise ValueError('invalid version')
    alive={};metadata=None;state=None;digest=hashlib.sha256();logs=[]
    for number in range(version+1):
        path=log/f'{number:020d}.json'
        if not path.exists():raise ValueError('complete JSON history required')
        raw=path.read_bytes();digest.update(raw)
        logs.append({'path':str(path),'sha256':hashlib.sha256(raw).hexdigest()})
        for line in raw.decode('utf-8').splitlines():
            if not line.strip():continue
            action=json.loads(line)
            if not isinstance(action,dict):raise ValueError('invalid Delta action')
            if 'metaData' in action:metadata=action['metaData']
            if 'commitInfo' in action and action['commitInfo'].get('userMetadata'):
                user=json.loads(action['commitInfo']['userMetadata'])
                if 'monthly_silver' in user:state=user['monthly_silver']
            if 'add' in action:
                item=action['add']
                if item.get('deletionVector'):raise ValueError('deletion vectors require a Delta reader')
                alive[item['path']]=item
            if 'remove' in action:alive.pop(action['remove']['path'],None)
    if metadata is None:raise ValueError('Delta metadata missing')
    files=[]
    for name,item in sorted(alive.items()):
        path=(root/unquote(name)).resolve()
        if root not in path.parents:raise ValueError('unsafe Delta path')
        stat=path.stat()
        if stat.st_size!=item['size']:raise ValueError('Delta file size changed')
        files.append({'path':str(path),'relative_path':name,'bytes':stat.st_size,'mtime_ns':stat.st_mtime_ns,
                      'partition_values':item.get('partitionValues',{})})
    return {'path':str(root),'version':version,'id':metadata['id'],'log_sha256':digest.hexdigest(),
            'properties':metadata.get('configuration',{}),'schema':json.loads(metadata['schemaString']),
            'monthly_state':state,'files':files,'logs':logs}


def check_pin(pinned):
    now=pin_delta(pinned['path'])
    if now['version']!=pinned['version']:raise ValueError('source advanced')
    if now!=pinned:raise ValueError('pinned source changed')


def verify_bundle(root):
    root=Path(root).resolve()
    result=json.loads((root/'result.json').read_text(encoding='utf-8'))
    if result.get('completed') is not True:raise ValueError('incomplete baseline')
    for name,expected in result['artifacts'].items():
        path=(root/name).resolve()
        if root not in path.parents or sha256(path)!=expected:raise ValueError('baseline artifact changed: '+name)
    return result
