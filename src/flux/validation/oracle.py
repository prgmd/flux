import hashlib
import math
from collections import Counter,defaultdict


KEYS={'song_metrics':['period_start','song_key'],'artist_metrics':['period_start','artist_mbid'],
      'artist_song_metrics':['period_start','artist_mbid','song_key'],'variant_metrics':['period_start','variant'],
      'listener_metrics':['period_start','listener_key'],'coverage':['period_start'],
      'identity_coverage':['period_start','identity_status']}


def compare_rows(expected,actual,keys):
    def index(rows):
        result={}
        for row in rows:
            key=tuple(str(row[k]) if row[k] is not None else None for k in keys)
            if key in result:raise ValueError('duplicate comparison key')
            result[key]=row
        return result
    left,right=index(expected),index(actual)
    if left.keys()!=right.keys():raise ValueError('row mismatch: missing or extra keys')
    for key,row in left.items():
        other=right[key]
        if row.keys()!=other.keys():raise ValueError('row mismatch: columns')
        for name,value in row.items():
            found=other[name]
            if isinstance(value,float):equal=isinstance(found,(int,float)) and math.isclose(value,found,rel_tol=1e-12,abs_tol=1e-12)
            elif name=='period_start':equal=str(value)==str(found)
            else:equal=type(value) is type(found) and value==found
            if not equal:raise ValueError('row mismatch: '+name)


def _metrics(users):
    values=list(users.values());total=sum(values);squares=sum(v*v for v in values)
    return {'listens':total,'listeners':len(values),'top_user_listens':max(values),
            'top_user_share':max(values)/total,'plays_per_listener':total/len(values),
            'concentration_hhi':squares/(total*total),'effective_listeners':total*total/squares}


def _rank(rows,groups,field,output):
    buckets=defaultdict(list)
    for row in rows:buckets[tuple(row[k] for k in groups)].append(row)
    for bucket in buckets.values():
        counts=Counter(r[field] for r in bucket);ranks={};prior=0
        for value,count in sorted(counts.items(),reverse=True):ranks[value]=prior+1;prior+=count
        for row in bucket:row[output]=ranks[row[field]]


def python_marts(monthly,dimension,labels,credits,period):
    if period not in ('month','year'):raise ValueError('unsupported period')
    dim={r['recording_msid']:r for r in dimension};lab={r['song_key']:r for r in labels}
    credit=defaultdict(dict);names={}
    for row in credits:
        credit[row['song_key']][row['artist_mbid']]=row['artist_name']
        names[row['artist_mbid']]=min(names.get(row['artist_mbid'],row['artist_name']),row['artist_name'])
    us=defaultdict(int)
    for row in monthly:
        date=str(row['listened_month'])[:7]+'-01' if period=='month' else str(row['listened_month'])[:4]+'-01-01'
        us[(date,row['user_name'],dim[row['recording_msid']]['song_key'])]+=int(row['play_count'])
    song=defaultdict(lambda:defaultdict(int));artist=defaultdict(lambda:defaultdict(int))
    variant=defaultdict(lambda:defaultdict(int));identity=defaultdict(lambda:defaultdict(int))
    listeners=defaultdict(dict);coverage=defaultdict(lambda:{'all':defaultdict(int),'linked':defaultdict(int)})
    for (date,user,key),plays in us.items():
        song[(date,key)][user]+=plays
        variant[(date,lab[key]['variant'])][user]+=plays
        identity[(date,lab[key]['identity_status'])][user]+=plays
        listeners[(date,user)][key]=plays;coverage[date]['all'][user]+=plays
        if credit[key]:coverage[date]['linked'][user]+=plays
        for aid in credit[key]:artist[(date,aid)][user]+=plays
    songs=[{'period_start':date,'song_key':key,**_metrics(users),**{k:v for k,v in lab[key].items() if k!='song_key'}} for (date,key),users in song.items()]
    for field,output in [('listeners','listener_rank'),('listens','listen_rank')]:_rank(songs,['period_start'],field,output)
    artists=[{'period_start':date,'artist_mbid':aid,**_metrics(users),'artist_name':names[aid]} for (date,aid),users in artist.items()]
    for field,output in [('listeners','listener_rank'),('listens','listen_rank')]:_rank(artists,['period_start'],field,output)
    by_artist={(r['period_start'],r['artist_mbid']):r for r in artists}
    artist_songs=[]
    for row in songs:
        for aid,name in credit[row['song_key']].items():
            a=by_artist[(row['period_start'],aid)]
            artist_songs.append({**row,'artist_mbid':aid,'artist_name':name,
                                 'song_listen_share':row['listens']/a['listens'],'song_listener_reach':row['listeners']/a['listeners']})
    _rank(artist_songs,['period_start','artist_mbid'],'listeners','artist_song_rank')
    cov=[]
    for date,groups in coverage.items():
        all_users,linked=groups['all'],groups['linked'];total=sum(all_users.values())
        cov.append({'period_start':date,'period_listens':total,'period_users':len(all_users),
                    'linked_listens':sum(linked.values()),'linked_users':len(linked),'artist_coverage':sum(linked.values())/total})
    by_period={r['period_start']:r for r in cov}
    variants=[]
    for (date,key),users in variant.items():
        values=_metrics(users);p=by_period[date]
        variants.append({'period_start':date,'variant':key,**values,'listen_share':values['listens']/p['period_listens'],
                         'listener_reach':values['listeners']/p['period_users']})
    return {'song_metrics':songs,'artist_metrics':artists,'artist_song_metrics':artist_songs,'coverage':cov,
            'variant_metrics':variants,
            'listener_metrics':[{'period_start':date,'listener_key':hashlib.sha256(user.encode()).hexdigest(),
              'listens':sum(values.values()),'song_count':len(values),'top_song_share':max(values.values())/sum(values.values())} for (date,user),values in listeners.items()],
            'identity_coverage':[{'period_start':date,'identity_status':status,'listens':sum(users.values()),'listeners':len(users)} for (date,status),users in identity.items()]}
