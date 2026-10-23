import re
from collections import Counter

KEYS={'monthly':('user_name','recording_msid','listened_month'),'dimension':('recording_msid',),
      'labels':('song_key',),'credits':('song_key','artist_mbid')}


def month(value):
    if not isinstance(value,str) or not re.fullmatch(r'(?!0000)[0-9]{4}-(?:0[1-9]|1[0-2])',value):
        raise ValueError('invalid UTC listening month')
    return value


def index(data,name):
    result={}
    for row in data[name]:
        key=tuple(row.get(field) for field in KEYS[name])
        if any(not isinstance(value,str) or not value.strip() for value in key) or key in result:
            raise ValueError('missing or duplicate source keys')
        if name=='monthly':
            month(row['listened_month'])
            if type(row.get('play_count')) is not int or row['play_count']<1:raise ValueError('invalid monthly contribution')
        result[key]=row
    return result


def snapshots(data):
    result={name:index(data,name) for name in KEYS}
    labels={key[0] for key in result['labels']}
    if (any((row['recording_msid'],) not in result['dimension'] for row in data['monthly'])
            or any(row.get('song_key') not in labels for row in data['dimension'])
            or any(row['song_key'] not in labels for row in data['credits'])):raise ValueError('unresolved source reference')
    return result


def changed(left,right):
    return {key for key in set(left)|set(right) if left.get(key)!=right.get(key)}


def affected_periods(before,after,*,policy_before,policy_after):
    old,new=snapshots(before),snapshots(after)
    all_months={row['listened_month'] for data in [before,after] for row in data['monthly']}
    changes=changed(old['monthly'],new['monthly'])
    affected={key[2] for key in changes}
    msids={key[0] for key in changed(old['dimension'],new['dimension'])}
    songs={key[0] for key in changed(old['labels'],new['labels'])}
    artists={key[1] for key in changed(old['credits'],new['credits'])}
    songs.update(key[0] for key in changed(old['credits'],new['credits']))
    for data in [before,after]:
        songs.update(row['song_key'] for row in data['dimension'] if row['recording_msid'] in msids)
        songs.update(row['song_key'] for row in data['credits'] if row['artist_mbid'] in artists)
    for data in [before,after]:
        ids=msids|{row['recording_msid'] for row in data['dimension'] if row['song_key'] in songs}
        affected.update(row['listened_month'] for row in data['monthly'] if row['recording_msid'] in ids)
    full=(not isinstance(policy_before,str) or not policy_before.strip() or not isinstance(policy_after,str)
          or not policy_after.strip() or policy_before!=policy_after)
    if full:affected=all_months
    return {'months':sorted(affected),'years':sorted({value[:4] for value in affected}),
            'requires_full_rebuild':full,'changed_song_keys':sorted(songs),'changed_artists':sorted(artists),
            'input_mode':'snapshot-diff','changed_monthly_keys':len(changes)}


def cdf_periods(rows,*,source_id,previous_source_id,previous_version,ending_version,range_complete):
    if (not source_id or source_id!=previous_source_id or range_complete is not True
            or type(previous_version) is not int or type(ending_version) is not int
            or previous_version<0 or ending_version<previous_version):raise ValueError('complete compatible CDF range required')
    periods=set();updates=Counter()
    for row in rows:
        version=row.get('_commit_version');kind=row.get('_change_type')
        if (type(version) is not int or not previous_version<version<=ending_version
                or kind not in {'insert','delete','update_preimage','update_postimage'}):raise ValueError('invalid CDF range or action')
        periods.add(month(row.get('listened_month')))
        if kind.startswith('update_'):updates[(version,kind)]+=1
    if any(updates[(version,'update_preimage')]!=updates[(version,'update_postimage')] for version,_ in updates):
        raise ValueError('incomplete CDF update pair')
    return {'months':sorted(periods),'years':sorted({value[:4] for value in periods})}
