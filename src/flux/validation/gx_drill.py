import json
import shutil
from copy import deepcopy
from pathlib import Path

from flux.validation.gx_pilot import save,validate
from flux.validation.pinning import sha256
from flux.validation.quality_gate import promote,verify_artifacts


def drill(validated,output):
    validated,output=Path(validated).resolve(),Path(output).resolve()
    if validated==output or validated in output.parents or output in validated.parents:raise ValueError('overlapping drill paths')
    source=json.loads((validated/'result.json').read_text(encoding='utf-8'))
    if source.get('completed') is not True:raise ValueError('successful GX sample required')
    verify_artifacts(validated,source['artifacts']);before=sha256(validated/'result.json')
    data=json.loads((validated/'input-snapshot.json').read_text(encoding='utf-8'))
    output.mkdir(parents=True,exist_ok=False)
    good=validate(data,output/'good')
    if not good['completed']:raise ValueError('normal drill control failed')
    pointer=output/'current.json';promote(output/'good',pointer,sandbox_root=output);pointer_before=pointer.read_bytes()
    cases=[]
    for fault in ['duplicate','blank_key','zero','boolean','wrong_rank']:
        changed=deepcopy(data)
        if fault=='duplicate':changed['monthly'].append(deepcopy(changed['monthly'][0]))
        elif fault=='blank_key':changed['monthly'][0]['user_name']=' '
        elif fault=='zero':changed['monthly'][0]['play_count']=0
        elif fault=='boolean':changed['monthly'][0]['play_count']=True
        else:changed['marts']['year']['song_metrics'][0]['listener_rank']=999999
        result=validate(changed,output/fault)
        blocked=False
        try:promote(output/fault,pointer,sandbox_root=output)
        except ValueError:blocked=True
        if not blocked or result['completed'] or result['gx']['completed'] is not True:raise ValueError('fault was not checked and blocked')
        if pointer.read_bytes()!=pointer_before:raise ValueError('failed result changed previous pointer')
        cases.append({'fault':fault,'blocked':True,'gx_passed':result['gx']['passed'],'contract_passed':result['contract']['quality_passed']})
    tampered=output/'tampered';shutil.copytree(output/'good',tampered)
    with (tampered/'ownership.json').open('a',encoding='utf-8') as stream:stream.write(' ')
    try:promote(tampered,pointer,sandbox_root=output)
    except ValueError:cases.append({'fault':'artifact_changed','blocked':True})
    else:raise ValueError('tampered candidate was published')
    verify_artifacts(validated,source['artifacts'])
    unchanged=sha256(validated/'result.json')==before and pointer.read_bytes()==pointer_before
    report={'completed':unchanged,'cases':cases,'monthly_rows':len(data['monthly']),'input_unchanged':unchanged,
            'previous_pointer_preserved':pointer.read_bytes()==pointer_before,'production_applied':False}
    save(output/'result.json',report)
    return report
