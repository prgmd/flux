import json
import uuid
from pathlib import Path

from flux.validation.pinning import sha256
from flux.validation.quality_contract import registry

GX_RULES={'DQ003','DQ004','DQ005'}


def ownership():
    engines={'DQ001':'input-guard','DQ002':'input-guard','DQ006':'dbt','DQ007':'profile',
             'DQ008':'spark-python','DQ009':'spark-python','DQ010':'dbt','DQ011':'dbt','DQ012':'independent-oracle',
             'DQ013':'dbt','DQ014':'review-journal','DQ015':'profile','DQ016':'collection','DQ017':'profile'}
    return [{'rule_id':row['rule_id'],'engine':'gx' if row['rule_id'] in GX_RULES else engines[row['rule_id']],
             'sample_oracle':'flux.validation.quality_contract'} for row in registry()['rules']]


def complete(contract,gx,*,artifacts_verified):
    rules=registry()['rules'];checks=contract.get('checks',[])
    required={row['rule_id']:row for row in rules}
    ok=(contract.get('evaluation_completed') is True and contract.get('quality_passed') is True
        and len(checks)==len(required) and {row.get('rule_id') for row in checks}==set(required))
    for row in checks:
        expected=required.get(row.get('rule_id'))
        if expected is None or row.get('severity')!=expected['severity']:ok=False;continue
        if row.get('status')=='SKIP':
            ok &= row['rule_id']=='DQ008' and row.get('observed',{}).get('reason')=='no declared valid detailed table in monthly baseline'
        else:ok &= row.get('status') in ({'PASS','WARN'} if row['severity']=='warning' else {'PASS'})
    engine=gx.get('checks',[])
    gx_ok=(gx.get('completed') is True and gx.get('passed') is True and not gx.get('error')
           and len(engine)==len(GX_RULES) and {row.get('rule_id') for row in engine}==GX_RULES
           and all(row.get('success') is True for row in engine))
    passed=bool(ok and gx_ok and artifacts_verified is True)
    return {'completed':passed,'publishable':passed,'production_applied':False,'promotion_ready':False,
            'scope':'isolated fixed sample','contract_passed':bool(ok),'gx_passed':bool(gx_ok),'artifacts_verified':artifacts_verified is True}


def verify_artifacts(root,artifacts):
    root=Path(root).resolve()
    if not isinstance(artifacts,dict) or not artifacts:raise ValueError('missing candidate artifacts')
    for name,digest in artifacts.items():
        file=(root/name).resolve()
        if root not in file.parents or not file.is_file() or sha256(file)!=digest:raise ValueError('candidate artifact changed')
    return True


def promote(candidate,pointer,*,sandbox_root):
    candidate,pointer,sandbox_root=map(lambda p:Path(p).resolve(),[candidate,pointer,sandbox_root])
    if (candidate==sandbox_root or sandbox_root not in candidate.parents or sandbox_root not in pointer.parents
            or candidate in pointer.parents):raise ValueError('explicit isolated sandbox paths required')
    result=json.loads((candidate/'result.json').read_text(encoding='utf-8'))
    verified=verify_artifacts(candidate,result.get('artifacts'))
    gate=complete(result.get('contract',{}),result.get('gx',{}),artifacts_verified=verified)
    if result.get('completed') is not True or result.get('publishable') is not True or not gate['publishable']:
        raise ValueError('candidate failed quality gate')
    pointer.parent.mkdir(parents=True,exist_ok=True)
    state={'snapshot':str(candidate),'result_sha256':sha256(candidate/'result.json'),'sandbox':str(sandbox_root)}
    temp=pointer.with_name(pointer.name+'.'+uuid.uuid4().hex+'.tmp')
    temp.write_text(json.dumps(state,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
    temp.replace(pointer)
    return state
