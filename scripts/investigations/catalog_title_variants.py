import argparse
import hashlib
import json
import time
from pathlib import Path

from flux.common import deltalog


SUFFIX = r'\s*(?:\(([^()]*)\)|\[([^\[\]]*)\]| - ([^-]+))\s*$'
CATEGORIES = [
    ('speed', r'\b(sped[ -]?up|speed[ -]?up|slowed|slow[ -]?down|nightcore|daycore|reverb|pitch)\b'),
    ('live', r'\b(live|concert|session|unplugged)\b'),
    ('remix', r'\b(remix|mix|mashup|bootleg)\b'),
    ('remaster', r'\b(remaster|remastered|remastering)\b'),
    ('acoustic', r'\b(acoustic|piano|orchestral)\b'),
    ('instrumental', r'\b(instrumental|karaoke|acapella|acappella|a cappella|drumless)\b'),
    ('content_label', r'\b(explicit|clean|censored|uncensored)\b'),
    ('edit', r'\b(edit|extended|short|mono|stereo|demo|rerecorded|re-recorded)\b'),
    ('credit', r'\b(feat|ft|featuring|with)\b'),
    ('edition', r'\b(version|ver|edition|deluxe|anniversary|bonus)\b'),
]


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def build(c):
    cases = ' '.join(f'when regexp_matches(qualifier,{literal(pattern)}) then {literal(name)}' for name, pattern in CATEGORIES)
    c.execute(f"""create or replace table title_rows as
        with normalized as (
            select distinct recording_msid,artist_name,track_name,
                nullif(trim(recording_mbid),'') recording_mbid,
                lower(trim(artist_name)) artist,lower(trim(track_name)) title
            from metadata where nullif(trim(artist_name),'') is not null
                and nullif(trim(track_name),'') is not null
        ), extracted as (
            select *,coalesce(nullif(regexp_extract(title,{literal(SUFFIX)},1),''),
                nullif(regexp_extract(title,{literal(SUFFIX)},2),''),
                nullif(regexp_extract(title,{literal(SUFFIX)},3),''),'') qualifier,
                trim(regexp_replace(title,{literal(SUFFIX)},'')) base_title from normalized
        ) select *,case when qualifier='' then 'none' {cases} else 'unclassified' end category
        from extracted""")
    print('title rows ready', flush=True)
    c.execute("""create or replace table qualifier_inventory as select qualifier,category,
        count(*) metadata_variants,count(distinct recording_msid) msids
        from title_rows where qualifier<>'' group by all""")
    c.execute("""create or replace table title_candidates as select artist,base_title,
        count(distinct title) title_count,count(distinct recording_msid) msids,
        count(distinct recording_mbid) mbids
        from title_rows where base_title<>'' group by artist,base_title
        having count(distinct title)>1 and count(*) filter(where qualifier<>'')>0""")
    print('suffix candidates ready', flush=True)
    c.execute("""create or replace table mbid_candidates as select artist,recording_mbid,
        count(distinct title) title_count,count(distinct recording_msid) msids
        from title_rows where recording_mbid is not null group by artist,recording_mbid
        having count(distinct title)>1""")
    print('MBID title variants ready', flush=True)


def fingerprint(path):
    logs = deltalog.commits(path)
    return {'version': int(logs[-1].stem), 'sha256': hashlib.sha256(b''.join(p.read_bytes() for p in logs)).hexdigest()}


def run(source, output):
    import duckdb

    source, output = source.resolve(), output.resolve()
    if source == output or source in output.parents or output in source.parents:
        raise ValueError('output overlaps source')
    output.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    before = fingerprint(source)
    c = duckdb.connect(str(output / 'catalog.duckdb'))
    c.execute('set threads=2')
    c.execute("set memory_limit='8GB'")
    c.execute("set max_temp_directory_size='30GB'")
    c.execute('set preserve_insertion_order=false')
    c.read_parquet(deltalog.active_paths(source), hive_partitioning=True).create_view('metadata')
    build(c)
    exports = {
        'qualifier_inventory': 'select * from qualifier_inventory',
        'title_candidates': 'select * from title_candidates',
        'title_members': 'select r.* from title_rows r join title_candidates using(artist,base_title)',
        'mbid_candidates': 'select * from mbid_candidates',
        'mbid_members': 'select r.* from title_rows r join mbid_candidates using(artist,recording_mbid)',
    }
    counts = {}
    for name, query in exports.items():
        c.execute(f'copy ({query}) to {literal(str(output / (name + ".parquet")))} (format parquet)')
        counts[name] = c.execute(f'select count(*) from ({query})').fetchone()[0]
    categories = c.execute('''select category,count(*) qualifier_types,sum(metadata_variants),sum(msids)
        from qualifier_inventory group by category order by 3 desc''').fetchall()
    examples = {}
    for category in [name for name, _ in CATEGORIES] + ['unclassified']:
        examples[category] = c.execute('''select distinct r.artist_name,r.track_name,r.base_title,r.recording_msid,r.recording_mbid
            from title_rows r join title_candidates using(artist,base_title)
            where category=? order by artist_name,track_name limit 15''',[category]).fetchall()
    after = fingerprint(source)
    result = {'completed': before == after, 'source':str(source),'source_before':before,'source_after':after,
              'source_unchanged':before == after,'source_rows':c.execute('select count(*) from metadata').fetchone()[0],
              'counts':counts,'categories':categories,'examples':examples,'seconds':round(time.monotonic()-started,2),
              'scope':'full metadata scan, final single qualifier and same-artist MBID title candidates; not verified identity'}
    (output / 'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding='utf-8')
    c.close()
    if before != after:
        raise ValueError('source changed')
    print(json.dumps({k:v for k,v in result.items() if k != 'examples'}),flush=True)
    return result


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--source',type=Path,required=True)
    p.add_argument('--output',type=Path,required=True)
    args = p.parse_args()
    run(args.source,args.output)
