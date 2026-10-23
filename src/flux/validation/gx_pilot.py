import json
import os
import time
from importlib.metadata import version
from pathlib import Path

from flux.validation.pinning import sha256,verify_bundle
from flux.validation.quality_contract import evaluate,load_baseline
from flux.validation.quality_gate import complete,ownership,verify_artifacts


def save(path,value):
    Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False,default=str)+'\n',encoding='utf-8')


def engine(data,root):
    os.environ['GX_ANALYTICS_ENABLED']='false'
    import great_expectations as gx
    import pandas as pd
    from great_expectations.checkpoint import UpdateDataDocsAction

    context=gx.get_context(mode='file',project_root_dir=str(root/'gx'))
    context.enable_analytics(False)
    source=context.data_sources.add_pandas('monthly_sample')
    batch=source.add_dataframe_asset(name='monthly').add_batch_definition_whole_dataframe('fixed_snapshot')
    columns=['user_name','recording_msid','listened_month','play_count']
    frame=pd.DataFrame.from_records(data['monthly'],columns=columns)
    frame['keys_are_strings']=[all(isinstance(row.get(name),str) for name in columns[:3]) for row in data['monthly']]
    frame['count_is_integer']=[type(row.get('play_count')) is int for row in data['monthly']]
    frame['numeric_count']=pd.to_numeric(frame['play_count'],errors='coerce')
    rules={
        'DQ003':[gx.expectations.ExpectTableRowCountToBeBetween(min_value=1),
                 gx.expectations.ExpectColumnValuesToBeInSet(column='keys_are_strings',value_set=[True]),
                 *[gx.expectations.ExpectColumnValuesToNotBeNull(column=name) for name in columns[:3]],
                 *[gx.expectations.ExpectColumnValuesToMatchRegex(column=name,regex=r'\S') for name in columns[:2]],
                 gx.expectations.ExpectColumnValuesToMatchRegex(column='listened_month',regex=r'^(?!0000)[0-9]{4}-(?:0[1-9]|1[0-2])\Z')],
        'DQ004':[gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=columns[:3],ignore_row_if='never')],
        'DQ005':[gx.expectations.ExpectColumnValuesToBeInSet(column='count_is_integer',value_set=[True]),
                 gx.expectations.ExpectColumnValuesToNotBeNull(column='numeric_count'),
                 gx.expectations.ExpectColumnValuesToBeBetween(column='numeric_count',min_value=1)]}
    definitions=[]
    for name,expectations in rules.items():
        suite=context.suites.add(gx.ExpectationSuite(name=name))
        for expectation in expectations:suite.add_expectation(expectation)
        definitions.append(context.validation_definitions.add(gx.ValidationDefinition(name=name,data=batch,suite=suite)))
    checkpoint=context.checkpoints.add(gx.Checkpoint(name='fixed_sample_quality',validation_definitions=definitions,
        actions=[UpdateDataDocsAction(name='local_docs')],result_format={'result_format':'BOOLEAN_ONLY'}))
    result=checkpoint.run(batch_parameters={'dataframe':frame})
    raw={'success':bool(result.success),'validation_results':[value.to_json_dict() for value in result.run_results.values()]}
    save(root/'gx-result.json',raw)
    checks=[]
    for validation in result.run_results.values():
        name=validation.suite_name
        checks.append({'rule_id':name,'success':bool(validation.success),'expectations':len(validation.results)})
    checks.sort(key=lambda row:row['rule_id'])
    documented=bool(list((root/'gx').rglob('index.html')))
    return {'completed':documented,'passed':bool(result.success),'checks':checks,'runtime':{
        name:version(name) for name in ['great-expectations','pandas','duckdb']},'data_docs_generated':documented,
        'analytics_enabled':False,'result_format':'BOOLEAN_ONLY','engine':'pandas bounded sample'}


def validate(data,output,*,verify_inputs=None):
    if len(data.get('monthly',[]))>10000:raise ValueError('GX pilot accepts at most 10000 monthly sample rows')
    root=Path(output).resolve();root.mkdir(parents=True,exist_ok=False)
    start=time.perf_counter()
    save(root/'input-snapshot.json',data)
    contract=evaluate(data);save(root/'contract.json',contract);save(root/'ownership.json',ownership())
    try:gx=engine(data,root)
    except Exception as error:gx={'completed':False,'passed':False,'checks':[],'error':type(error).__name__}
    save(root/'engine.json',gx)
    artifacts={path.relative_to(root).as_posix():sha256(path) for path in root.rglob('*') if path.is_file()}
    verified=verify_artifacts(root,artifacts)
    try:
        if verify_inputs is not None:verify_inputs()
    except Exception:verified=False
    result={**complete(contract,gx,artifacts_verified=verified),'contract':contract,'gx':gx,'artifacts':artifacts,
            'monthly_rows':len(data['monthly']),'rule_ownership':ownership(),'seconds':round(time.perf_counter()-start,3)}
    save(root/'result.json',result)
    return result


def run(baseline,output,*,candidate_db=None):
    import duckdb
    from flux.validation.baseline import rows
    from flux.validation.dbt_pilot import MARTS

    baseline,output=Path(baseline).resolve(),Path(output).resolve()
    if baseline==output or baseline in output.parents or output in baseline.parents:raise ValueError('overlapping baseline and output')
    verify_bundle(baseline);before=sha256(baseline/'result.json')
    paths=list((baseline/'inputs').glob('*.parquet'))+list((baseline/'expected').rglob('*.parquet'))
    with duckdb.connect() as connection:
        connection.execute('set threads=2');connection.execute("set memory_limit='1GB'")
        counts=[connection.execute('select count(*) from read_parquet(?)',[str(path)]).fetchone()[0] for path in paths]
        if any(count>100000 for count in counts) or sum(counts)>1000000:raise ValueError('bounded sample required')
        if any(path.name=='monthly.parquet' and count>10000 for path,count in zip(paths,counts)):
            raise ValueError('bounded monthly sample required')
    data=load_baseline(baseline);candidate_before=None;candidate_manifest=None;candidate_result=None
    if candidate_db is not None:
        candidate_db=Path(candidate_db).resolve()
        if candidate_db==output or candidate_db in output.parents or output in candidate_db.parents:raise ValueError('overlapping candidate output')
        candidate_info=json.loads((candidate_db.parent/'result.json').read_text(encoding='utf-8'))
        inputs=json.loads((candidate_db.parent/'inputs.json').read_text(encoding='utf-8'))
        candidate_manifest=sha256(candidate_db.parent/'inputs.json');candidate_result=sha256(candidate_db.parent/'result.json')
        if (candidate_info.get('completed') is not True or candidate_info.get('input_manifest_sha256')!=candidate_manifest
                or inputs.get('baseline_sha256')!=before
                or candidate_info.get('inputs_unchanged') is not True):raise ValueError('candidate dbt input differs')
        verify_artifacts(candidate_db.parent,inputs.get('artifacts'))
        candidate_before=sha256(candidate_db)
        with duckdb.connect(str(candidate_db),read_only=True) as connection:
            for period,names in MARTS.items():
                for name,table in names.items():
                    table='analytics.'+table
                    if connection.execute('select count(*) from '+table).fetchone()[0]>100000:raise ValueError('bounded candidate required')
                    data['marts'][period][name]=rows(connection,table)
    def verify():
        verify_bundle(baseline)
        if sha256(baseline/'result.json')!=before:raise ValueError('baseline changed')
        if candidate_before is not None and sha256(candidate_db)!=candidate_before:raise ValueError('candidate database changed')
        if candidate_manifest is not None:
            if sha256(candidate_db.parent/'inputs.json')!=candidate_manifest or sha256(candidate_db.parent/'result.json')!=candidate_result:
                raise ValueError('candidate manifest changed')
            verify_artifacts(candidate_db.parent,inputs['artifacts'])
    result=validate(data,output,verify_inputs=verify)
    result.update(baseline_manifest_sha256=before,source_access_required=False,baseline_unchanged=result['artifacts_verified'],
                  candidate_database_sha256=candidate_before,candidate_kind='dbt snapshot' if candidate_db else 'frozen expectations')
    save(output/'result.json',result)
    return result
