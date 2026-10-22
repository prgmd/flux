import json
import shutil
import tarfile
from pathlib import Path

from flux.validation.dumps import dump_header
from flux.validation.pinning import sha256,verify_bundle


def sample_archive(path,output,limit):
    from compression import zstd

    count=0
    with zstd.ZstdFile(path) as stream,tarfile.open(fileobj=stream,mode='r|') as archive,Path(output).open('x',encoding='utf-8',newline='\n') as target:
        for member in archive:
            if not member.name.endswith('.listens'):continue
            for line in archive.extractfile(member):
                text=line.decode('utf-8').strip()
                if not text:continue
                if not isinstance(json.loads(text),dict):raise ValueError('nonobject source record')
                target.write(text+'\n');count+=1
                if count==limit:return count
    if not count:raise ValueError('empty archive sample')
    return count


def prepare(baseline,output,*,limit=128):
    baseline,output=Path(baseline).resolve(),Path(output).resolve()
    if output==baseline or output in baseline.parents or baseline in output.parents:raise ValueError('overlapping baseline')
    if type(limit) is not int or not 1<=limit<=10000:raise ValueError('bounded sample required')
    verify_bundle(baseline);before=sha256(baseline/'result.json')
    inventory=json.loads((baseline/'dump_inventory.json').read_text(encoding='utf-8'))['records']
    pair=next(([a,b] for a,b in zip(inventory,inventory[1:]) if a['end']==b['start'] and a['has_listen_record'] and b['has_listen_record']),None)
    if pair is None:raise ValueError('no retained consecutive dumps')
    for row in pair:
        source=Path(row['path'])
        if output==source or output in source.parents or source in output.parents:raise ValueError('overlapping archive')
    output.mkdir(parents=True,exist_ok=False);inputs=output/'inputs';inputs.mkdir()
    shutil.copyfile(baseline/'inputs/bronze_probe.parquet',inputs/'base.parquet')
    records=[]
    for index,row in enumerate(pair):
        path=Path(row['path']);before_stat=path.stat()
        header=dump_header({'path':str(path),'size':row['bytes'],'dump':row['dump'],'checksums':row['recorded_checksums']})
        digest=sha256(path)
        if digest!=row['recorded_checksums']['sha256']:raise ValueError('source archive checksum mismatch')
        relative=f'inputs/dump-{index+1}.jsonl'
        count=sample_archive(path,output/relative,limit)
        after_stat=path.stat()
        if (before_stat.st_size,before_stat.st_mtime_ns)!=(after_stat.st_size,after_stat.st_mtime_ns):raise ValueError('archive changed')
        records.append({'dump':row['dump'],'path':str(path),'sha256':digest,'rows':count,'sample':relative,
                        'start':header['start'],'end':header['end'],'bytes':after_stat.st_size,'mtime_ns':after_stat.st_mtime_ns})
    if records[0]['end']!=records[1]['start']:raise ValueError('actual dump windows do not join')
    verify_bundle(baseline)
    if before!=sha256(baseline/'result.json'):raise ValueError('baseline changed')
    result={'prepared':True,'scope':'retained real dump prefixes and pinned Bronze probe; not fresh full dumps',
            'baseline_manifest_sha256':before,'baseline_unchanged':True,'production_applied':False,'archives':records,
            'artifacts':{p.relative_to(output).as_posix():sha256(p) for p in sorted(inputs.iterdir())}}
    (output/'inputs.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    return result
