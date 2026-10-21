import argparse
import hashlib
import json
import sys
import unicodedata as ud
from collections import Counter, defaultdict
from pathlib import Path

from flux.silver.name_character_catalog import describe, load_ranges, policy


SOURCES = {
    'Scripts.txt':'https://www.unicode.org/Public/16.0.0/ucd/Scripts.txt',
    'ScriptExtensions.txt':'https://www.unicode.org/Public/16.0.0/ucd/ScriptExtensions.txt',
    'NormalizationTest.txt':'https://www.unicode.org/Public/16.0.0/ucd/NormalizationTest.txt',
    'confusables.txt':'https://www.unicode.org/Public/security/16.0.0/confusables.txt',
}


def decode(text):
    return ''.join(chr(int(code,16)) for code in text.split())


def verify_normalization(path):
    tests=checks=0
    for line in path.read_text(encoding='utf-8').splitlines():
        text=line.split('#',1)[0].strip()
        if not text or text.startswith('@'):
            continue
        values=[decode(v.strip()) for v in text.split(';')[:5]]
        a,b,c,d,e=values
        expected={'NFC':[b,b,b,d,d],'NFD':[c,c,c,e,e],
                  'NFKC':[d]*5,'NFKD':[e]*5}
        for form,targets in expected.items():
            for value,target in zip(values,targets):
                if ud.normalize(form,value)!=target:
                    raise ValueError('normalization conformance failure')
                checks+=1
        tests+=1
    if not tests:
        raise ValueError('no normalization test cases')
    return {'cases':tests,'assertions':checks,'passed':True}


def build(sources, output):
    sources,output=sources.resolve(),output.resolve()
    if output==sources or sources in output.parents or output in sources.parents:
        raise ValueError('overlapping output')
    config=policy()
    if ud.unidata_version!=config['unicode_version']:
        raise ValueError('Unicode version does not match registry')
    descriptors={}
    for name,url in SOURCES.items():
        raw=(sources/name).read_bytes()
        if config['unicode_version'] not in raw[:4096].decode('utf-8'):
            raise ValueError('source version mismatch: '+name)
        descriptors[name]={'url':url,'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
    output.mkdir(parents=True,exist_ok=False)
    manifest={'completed':False,'unicode_version':ud.unidata_version,'python':sys.version,
              'policy_version':config['policy_version'],'source_files':descriptors,
              'production_applied':False,'music_corpus_scanned':False}
    report=output/'result.json'
    report.write_text(json.dumps(manifest,indent=2),encoding='utf-8')
    latin=load_ranges(sources/'Scripts.txt','Latin')|load_ranges(sources/'ScriptExtensions.txt','Latn')
    confusable_sources=set()
    confusables=[]
    for line in (sources/'confusables.txt').read_text(encoding='utf-8').splitlines():
        text=line.split('#',1)[0].strip()
        if not text:
            continue
        source,target,kind=(v.strip() for v in text.split(';')[:3])
        original,replacement=decode(source),decode(target)
        confusable_sources.update(original)
        confusables.append({'source':original,'target':replacement,'kind':kind,'action':'review_only'})
    counts=Counter();groups=defaultdict(list);included_latin=set()
    with (output/'characters.jsonl').open('w',encoding='utf-8') as stream:
        for point in range(0x110000):
            if 0xD800<=point<=0xDFFF:
                continue
            char=chr(point)
            row=describe(char,latin=point in latin,confusable=char in confusable_sources)
            if row is None:
                continue
            stream.write(json.dumps(row,ensure_ascii=False)+'\n')
            counts['characters']+=1
            counts[row['action']]+=1
            if point in latin:
                included_latin.add(point)
            for form in ('nfc','nfkc','lower','casefold'):
                counts[form+'_changes']+=row[form]!=char
            if row['latin_candidate'] is not None:
                groups[row['latin_candidate']].append(row['codepoint'])
    if latin!=included_latin:
        raise ValueError('Latin script coverage incomplete')
    manifest['normalization_tests']=verify_normalization(sources/'NormalizationTest.txt')
    (output/'confusables.json').write_text(json.dumps(confusables,ensure_ascii=False,indent=2),encoding='utf-8')
    (output/'latin_candidates.json').write_text(json.dumps(groups,ensure_ascii=False,indent=2),encoding='utf-8')
    (output/'policy.json').write_text(json.dumps(config,ensure_ascii=False,indent=2),encoding='utf-8')
    lines=['# 이름 전처리 대조표','',
           f"Unicode {ud.unidata_version}, 정책 {config['policy_version']}. 모든 값은 후보이며 자동 곡 병합을 뜻하지 않는다.",
           '', '| 규칙 | 예시 | 처리 | 적용 범위 |','| --- | --- | --- | --- |']
    for family in config['families']:
        examples=', '.join(json.dumps(s,ensure_ascii=False).replace('|','\\|')+
                           ' ['+' '.join(f'U+{ord(ch):04X}' for ch in s)+']' for s in family['examples'])
        lines.append('| '+family['id']+' | '+examples+' | '+family['action']+' | '+family['scope']+' |')
    (output/'matching-table.md').write_text('\n'.join(lines)+'\n',encoding='utf-8')
    manifest.update({'completed':True,'counts':dict(counts),'latin_script_or_extension_codepoints':len(latin),
                     'confusable_mappings':len(confusables),'latin_candidate_groups':len(groups),
                     'rule_families':len(config['families']),'scalar_values_examined':0x110000-0x800,
                     'registry_sha256':hashlib.sha256(json.dumps(config,sort_keys=True,ensure_ascii=False).encode()).hexdigest()})
    report.write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
    print(json.dumps(manifest,ensure_ascii=False))
    return manifest


if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--sources',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    a=p.parse_args();build(a.sources,a.output)
