from pathlib import Path

from flux.validation.dbt_increment import load_sources,previous_proof,save,update
from flux.validation.pinning import sha256


def metadata_policy_drill(previous,output,project):
    previous,output,project=map(lambda value:Path(value).resolve(),[previous,output,project])
    if any(source==output or source in output.parents or output in source.parents for source in [previous,project]):
        raise ValueError('separate drill output required')
    previous_proof(previous);data=load_sources(previous)
    if not data['credits'] or not data['labels']:raise ValueError('credited sample required')
    before=sha256(previous/'pilot.duckdb');plays=sum(row['play_count'] for row in data['monthly'])
    output.mkdir(parents=True,exist_ok=False)
    data['labels'][0]['song_name']='Controlled label for validation'
    artist=data['credits'][0]['artist_mbid']
    for row in data['credits']:
        if row['artist_mbid']==artist:row['artist_name']='Controlled artist label for validation'
    metadata=update(previous,output/'metadata',data,project)
    if not metadata['completed'] or metadata['checks']['plays']!=plays:raise ValueError('metadata update failed')
    full=update(output/'metadata',output/'policy',data,project,policy='controlled-policy-v2',allow_full_rebuild=True)
    if not full['completed'] or not full['impact']['requires_full_rebuild'] or full['checks']['plays']!=plays:
        raise ValueError('policy fallback failed')
    if sha256(previous/'pilot.duckdb')!=before:raise ValueError('previous database changed')
    result={'completed':True,'scope':'controlled metadata and policy tag, formulas unchanged','production_applied':False,
            'metadata':{key:metadata[key] for key in ['impact','checks','seconds','build_nodes']},
            'policy':{key:full[key] for key in ['impact','checks','seconds','build_nodes']},'previous_unchanged':True}
    save(output,result)
    return result
