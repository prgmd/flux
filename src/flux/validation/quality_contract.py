import hashlib
import json
import math
import re
from collections import Counter,defaultdict
from datetime import date,timezone
from importlib.resources import files
from pathlib import Path

from flux.silver.quality import MIN_MS_PLAYED,PLAYED_NUMBER_PATTERN,judge
from flux.validation.baseline import rows
from flux.validation.oracle import KEYS,compare_rows,python_marts
from flux.validation.pinning import sha256,verify_bundle


def registry():
    value=json.loads(files('flux.validation').joinpath('quality_rules.json').read_text(encoding='utf-8'))
    seen=set()
    for row in value['rules']:
        if row['rule_id'] in seen or row['severity'] not in ('error','warning','info'):raise ValueError('invalid rule registry')
        if not set(row['depends_on'])<=seen:raise ValueError('invalid rule dependency')
        seen.add(row['rule_id'])
    return value


def _integer(value):return type(value) is int and value>=1
def _blank(value):return not isinstance(value,str) or not value.strip()
def _judge(item):return judge(item['document'],ingested_at=item['ingested_at'])


def _required(data):
    invalid=0
    for row in data['monthly']:
        month=row.get('listened_month')
        try:
            valid=isinstance(month,str) and re.fullmatch(r'[0-9]{4}-[0-9]{2}',month) and date(int(month[:4]),int(month[5:7]),1)
        except ValueError:valid=False
        invalid+=bool(_blank(row.get('user_name')) or _blank(row.get('recording_msid')) or not valid)
    return invalid==0 and bool(data['monthly']),{'invalid_rows':invalid,'rows':len(data['monthly'])}


def _unique(data):
    keys=[(r['user_name'],r['recording_msid'],r['listened_month']) for r in data['monthly']]
    duplicates=len(keys)-len(set(keys))
    return duplicates==0,{'duplicate_keys':duplicates}


def _references(data):
    dim=[r['recording_msid'] for r in data['dimension']];labels=[r['song_key'] for r in data['labels']]
    credits=[(r['song_key'],r['artist_mbid']) for r in data['credits']]
    dim_keys,label_keys=set(dim),set(labels)
    problems=(len(dim)-len(dim_keys)+len(labels)-len(label_keys)+len(credits)-len(set(credits))
              +sum(_blank(key) for key in dim)+sum(_blank(key) for key in labels)
              +sum(_blank(r['song_key']) for r in data['dimension'])
              +sum(r['recording_msid'] not in dim_keys for r in data['monthly'])
              +sum(r['song_key'] not in label_keys for r in data['dimension'])
              +sum(r['song_key'] not in label_keys or _blank(r['song_key']) or _blank(r['artist_mbid']) for r in data['credits']))
    return problems==0,{'reference_or_duplicate_errors':problems}


def _conservation(data):
    total=sum(r['play_count'] for r in data['monthly']);bad=0
    for period in ['month','year']:
        for table,field in [('song_metrics','listens'),('coverage','period_listens'),('variant_metrics','listens'),
                            ('listener_metrics','listens'),('identity_coverage','listens')]:
            bad+=sum(r[field] for r in data['marts'][period][table])!=total
    return bad==0,{'input_plays':total,'mismatched_totals':bad,'artist_global_sum_constrained':False}


def _bounds(data):
    count_fields={'listens','listeners','top_user_listens','period_listens','period_users','linked_listens','linked_users',
                  'song_count','listener_rank','listen_rank','artist_song_rank','source_msids'}
    shares={'top_user_share','concentration_hhi','song_listen_share','song_listener_reach','artist_coverage',
            'listen_share','listener_reach','top_song_share'}
    bad=0
    for tables in data['marts'].values():
        for records in tables.values():
            for row in records:
                for key,value in row.items():
                    if key in count_fields:bad+=type(value) is not int or value<0
                    if key in shares:bad+=not isinstance(value,(float,int)) or isinstance(value,bool) or not math.isfinite(value) or value<0 or value>1+1e-12
                if 'listeners' in row and 'listens' in row:bad+=not (0<row['listeners']<=row['listens'])
                if 'top_user_listens' in row:bad+=not (0<row['top_user_listens']<=row['listens'])
                if 'effective_listeners' in row:
                    value=row['effective_listeners'];bad+=not math.isfinite(value) or value<1-1e-12 or value>row['listeners']+1e-12
                if 'plays_per_listener' in row:bad+=not math.isfinite(row['plays_per_listener']) or row['plays_per_listener']<1
                if 'concentration_hhi' in row:bad+=row['concentration_hhi']<=0
    return bad==0,{'invalid_metric_values':bad}


def _oracle(data):
    checked=0;bad=0
    for period in ['month','year']:
        expected=python_marts(data['monthly'],data['dimension'],data['labels'],data['credits'],period)
        for name,keys in KEYS.items():
            try:compare_rows(expected[name],data['marts'][period][name],keys)
            except (KeyError,ValueError):bad+=1
            checked+=1
    return bad==0,{'tables_checked':checked,'mismatched_tables':bad,'float_tolerance':1e-12}


def _versions(data):
    variants=defaultdict(set)
    for row in data['dimension']:variants[row['song_key']].add(row['variant'])
    bad=sum(row['variant']!=(next(iter(variants[row['song_key']])) if len(variants[row['song_key']])==1 else 'conflicting_labels') for row in data['labels'])
    return bad==0,{'mismatched_version_labels':bad}


def _duration_profile(data):
    missing=invalid=0
    for item in data['raw_probe']:
        doc=item['document'];meta=doc.get('track_metadata');additional=meta.get('additional_info') if isinstance(meta,dict) else None
        played=additional.get('ms_played') if isinstance(additional,dict) else None
        if played is None:missing+=1;continue
        try:
            numeric=(isinstance(played,(int,float,str)) and not isinstance(played,bool)
                     and (not isinstance(played,str) or re.fullmatch(PLAYED_NUMBER_PATTERN,played)) and math.isfinite(float(played)))
        except (ValueError,OverflowError):numeric=False
        invalid+=not numeric
    return invalid==0,{'rows':len(data['raw_probe']),'missing_allowed':missing,'unusable_duration':invalid}


def _profile(data,version):
    valid=[r for r in data.get('monthly',[]) if _integer(r.get('play_count'))]
    total=sum(r['play_count'] for r in valid);metadata=data.get('metadata',[])
    dim={r['recording_msid']:r['song_key'] for r in data.get('dimension',[])};linked={r['song_key'] for r in data.get('credits',[])}
    plays=sum(r['play_count'] for r in valid if dim.get(r.get('recording_msid')) in linked)
    probe=data.get('raw_probe',[])
    return {'scope_id':data.get('context',{}).get('scope_id'),'listener_key':data.get('context',{}).get('listener_key'),
            'time_basis':'UTC listening month','policy_version':version,
            'values':{'monthly_rows':len(data.get('monthly',[])),'plays':total,
                      'metadata_missing_mbid_rate':sum(r.get('recording_mbid') is None for r in metadata)/len(metadata) if metadata else None,
                      'linked_play_rate':plays/total if total else None,
                      'raw_probe_rejection_rate':sum(not _judge(r).ok for r in probe)/len(probe) if probe else None}}


def _profile_check(profile,reference):
    if reference is None:return False,{'state':'calibration_pending','temporal_history_available':False,'thresholds_for_live_data':None}
    if any(profile[k]!=reference.get(k) for k in ['scope_id','listener_key','time_basis','policy_version']):
        return False,{'state':'incomparable_reference','numeric_comparison_performed':False}
    changed=[key for key,value in profile['values'].items() if value!=reference.get('values',{}).get(key)]
    return not changed,{'state':'same_scope_comparison','changed_metrics':changed,'live_thresholds_calibrated':False}


def evaluate(data,reference_profile=None):
    policy=registry();results={}
    try:profile=_profile(data,policy['version'])
    except (ValueError,KeyError,TypeError,ZeroDivisionError):profile={'values':{},'scope_id':None}
    def check(name):
        if name=='integrity':return data.get('integrity') is True,{'artifact_integrity_verified':data.get('integrity') is True}
        if name=='context':
            c=data['context'];ok=(c['listener_key']=='user_name' and c['timezone']=='UTC' and type(c['minimum_ms_played']) is int
                and c['minimum_ms_played']==MIN_MS_PLAYED==30000 and not _blank(c.get('scope_id')) and c.get('snapshot_consistent',True) is True)
            return bool(ok),{'listener_key':c['listener_key'],'timezone':c['timezone'],'minimum_ms_played':c['minimum_ms_played'],'snapshot_consistent':c.get('snapshot_consistent',True)}
        if name=='required':return _required(data)
        if name=='unique':return _unique(data)
        if name=='counts':
            n=sum(not _integer(r.get('play_count')) for r in data['monthly']);return n==0,{'invalid_counts':n}
        if name=='references':return _references(data)
        if name=='missing_ids':return True,{'metadata_rows':len(data['metadata']),'missing_allowed':sum(r.get('recording_mbid') is None for r in data['metadata'])}
        if name=='valid_details':
            if not data.get('valid_details'):return None,{'reason':'no declared valid detailed table in monthly baseline'}
            n=sum(not _judge(r).ok for r in data['valid_details']);return n==0,{'rows':len(data['valid_details']),'invalid_records':n}
        if name=='quarantine':
            n=sum(_judge(r).ok or _judge(r).reason!=r['reason'] for r in data['quarantine']);return n==0,{'rows':len(data['quarantine']),'mismatches':n}
        if name=='conservation':return _conservation(data)
        if name=='bounds':return _bounds(data)
        if name=='oracle':return _oracle(data)
        if name=='versions':return _versions(data)
        if name=='review':
            r=data['r9_export'];ok=(data['context']['r9_reference_only'] is True and type(r['approved_candidates']) is int
                                  and r['approved_candidates']==0 and r['mappings']==[])
            return ok,{'approved_candidates':r['approved_candidates'],'mapping_rows':len(r['mappings']),'scope':'reference-only P0; nonempty exports require journal verification'}
        if name=='duration_profile':return _duration_profile(data)
        if name=='collection':
            r=data['dump_summary'];return not r['gaps'] and not r['overlaps'] and r['new_input_sequence_ready'] is True,{'gaps':len(r['gaps']),'overlaps':len(r['overlaps']),'future_input_ready':r['new_input_sequence_ready']}
        if name=='profile':return _profile_check(profile,reference_profile)
        raise ValueError('unimplemented rule')
    for rule in policy['rules']:
        blocked=[key for key in rule['depends_on'] if results[key]['status']!='PASS']
        if blocked:status='SKIP';observed={'blocked_by':blocked}
        else:
            try:
                ok,observed=check(rule['check'])
                status='SKIP' if ok is None else 'PASS' if ok else 'WARN' if rule['severity']=='warning' else 'FAIL'
            except (ValueError,KeyError,TypeError,OverflowError,ZeroDivisionError) as error:
                status='FAIL';observed={'error':'invalid_check_input','error_type':type(error).__name__}
        results[rule['rule_id']]={'rule_id':rule['rule_id'],'severity':rule['severity'],'status':status,'observed':observed}
    summary=dict(Counter(r['status'] for r in results.values()))
    return {'policy_version':policy['version'],'evaluation_completed':True,'quality_passed':summary.get('FAIL',0)==0,
            'promotion_ready':False,'scope':'fixed sample only','summary':summary,'checks':list(results.values()),'profile':profile}


def load_baseline(root):
    import duckdb
    from flux.gold.monthly_marts import literal

    root=Path(root);info=verify_bundle(root)
    source=json.loads((root/'source_context.json').read_text(encoding='utf-8'))
    compat=json.loads((root/'compatibility.json').read_text(encoding='utf-8'))
    states=[source['tables'][name]['monthly_state'] for name in ['monthly_user_recordings','recording_metadata','invalid_listens']]
    bronze=source['tables']['bronze']
    consistent=all(s==states[0] for s in states) and states[0]['source_id']==bronze['id'] and states[0]['version']==bronze['version']
    with duckdb.connect() as c:
        c.execute('set threads=2');c.execute("set memory_limit='4GB'")
        read=lambda path:rows(c,'read_parquet('+literal(path)+')')
        inputs={key:read(root/'inputs'/(name+'.parquet')) for key,name in [('monthly','monthly'),('dimension','song_dimension'),('labels','song_labels'),('credits','song_credits'),('metadata','recording_metadata')]}
        def records(name):
            return [{'document':json.loads(row['raw_json']),'ingested_at':row['_ingested_at'].replace(tzinfo=timezone.utc).timestamp(),
                     **({'reason':row['reason']} if 'reason' in row else {})} for row in read(root/'inputs'/(name+'.parquet'))]
        inputs.update(integrity=True,context={'listener_key':compat['listener_key'],'timezone':source['gold_policies']['timezone'],
            'minimum_ms_played':states[0]['minimum_ms_played'],'scope_id':sha256(root/'result.json'),
            'r9_reference_only':source['r9_reference_only'],'snapshot_consistent':consistent},
            marts={p:{name:read(root/'expected'/p/(name+'.parquet')) for name in KEYS} for p in ['month','year']},
            quarantine=records('quarantine_probe'),raw_probe=records('bronze_probe'),valid_details=None,
            r9_export=json.loads((root/'source_manifests/r9_production_export.json').read_text(encoding='utf-8')),
            dump_summary=json.loads((root/'dump_inventory.json').read_text(encoding='utf-8'))['summary'])
    return inputs


def run(baseline,output,reference_profile=None):
    import duckdb

    baseline,output=Path(baseline).resolve(),Path(output).resolve()
    if output==baseline or output in baseline.parents or baseline in output.parents:raise ValueError('overlapping report output')
    output.mkdir(parents=True,exist_ok=False)
    try:
        before=sha256(baseline/'result.json');data=load_baseline(baseline)
    except (ValueError,OSError,KeyError,TypeError,duckdb.Error):
        before=None;data={'integrity':False}
    result=evaluate(data,reference_profile)
    result.update(registry_sha256=hashlib.sha256(files('flux.validation').joinpath('quality_rules.json').read_bytes()).hexdigest(),
                  baseline_manifest_sha256=before,source_access_required=False,production_applied=False,baseline_unchanged=False)
    if before is not None:
        try:
            verify_bundle(baseline)
            if before!=sha256(baseline/'result.json'):raise ValueError('baseline changed')
            result['baseline_unchanged']=True
        except (ValueError,OSError):
            result['baseline_unchanged']=False;result['quality_passed']=False
            result['checks'][0].update(status='FAIL',observed={'artifact_integrity_verified':False,'reason':'changed_during_evaluation'})
            result['summary']=dict(Counter(r['status'] for r in result['checks']))
    (output/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    (output/'profile.json').write_text(json.dumps(result['profile'],ensure_ascii=False,indent=2,allow_nan=False)+'\n',encoding='utf-8')
    return result
