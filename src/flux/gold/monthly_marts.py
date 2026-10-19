VARIANTS = {
    'sped_up': r'\b(sped[ -]?up|speed[ -]?up|nightcore)\b',
    'slowed': r'\b(slowed|slow[ -]?down|daycore)\b',
    'remix': r'\b(remix|mashup|bootleg)\b',
    'live': r'\b(live|concert|session|unplugged)\b',
    'acoustic': r'\b(acoustic|orchestral)\b',
    'instrumental': r'\b(instrumental|karaoke|acapella|acappella|drumless)\b',
    'remaster': r'\b(remaster|remastered|remastering)\b',
    'clean': r'\b(clean|censored)\b',
    'explicit': r'\b(explicit|uncensored)\b',
    'edit': r'\b(edit|extended|mono|stereo|demo|rerecorded|re-recorded)\b',
    'effect': r'\b(reverb|pitch|bass boosted)\b',
}


def literal(value):
    return "'"+str(value).replace("'", "''")+"'"


def build_dimension(c):
    separate = 'artist_status' in [row[0] for row in c.execute('describe reviewed').fetchall()]
    credit_condition = "r.artist_status='accepted'" if separate else "d.status='matched_candidate'"
    terms = ','.join(f"case when regexp_matches(labels,{literal(pattern)}) then '{name}' end"
                     for name, pattern in VARIANTS.items())
    c.execute(f"""create or replace table song_dimension as
        with labelled as (select *, lower(concat_ws(' ',
            array_to_string(regexp_extract_all(title,'\\([^()]*\\)|\\[[^\\[\\]]*\\]| - .*$',0),' '),
            case when status in ('matched_candidate','version_review') then comment end)) labels
            from reviewed)
        select recording_msid,recording_mbid,status,
            case when status='matched_candidate' then 'mbid:'||recording_mbid
                 else 'msid:'||recording_msid end song_key,
            coalesce(case when status='matched_candidate' then recording_title end,title,'[unknown]') song_name,
            artist observed_artist,
            coalesce(nullif(concat_ws('+',{terms}),''),'unmarked') variant
        from labelled""")
    c.execute("""create or replace table song_labels as select song_key,
        min(song_name) song_name,min(observed_artist) observed_artist,
        case when count(distinct variant)=1 then min(variant) else 'conflicting_labels' end variant,
        min(status) identity_status,count(*) source_msids
        from song_dimension group by song_key""")
    c.execute(f"""create or replace table song_credits as select d.song_key,p.artist_mbid,
        min(p.artist_name) artist_name from song_dimension d join proposed_credits p using(recording_msid)
        join reviewed r using(recording_msid)
        where {credit_condition} group by d.song_key,p.artist_mbid""")


def metrics(c, name, source, keys):
    fields=','.join(keys)
    c.execute(f"""create or replace table {name} as
        select period_start,{fields},sum(plays)::bigint listens,count(*) listeners,
            max(plays) top_user_listens,max(plays)::double/sum(plays) top_user_share,
            sum(plays)::double/count(*) plays_per_listener,
            sum(plays::double*plays)/pow(sum(plays)::double,2) concentration_hhi,
            pow(sum(plays)::double,2)/sum(plays::double*plays) effective_listeners
        from {source} group by period_start,{fields}""")


def build_period(c, period):
    if period not in ('month','year'):
        raise ValueError('unsupported period')
    date="cast(substr(cast(listened_month as varchar),1,7)||'-01' as date)"
    c.execute(f"""create or replace table user_songs as select date_trunc('{period}',{date})::date period_start,
        user_name,song_key,sum(play_count)::bigint plays from monthly join song_dimension using(recording_msid)
        group by period_start,user_name,song_key""")
    metrics(c,'song_base','user_songs',['song_key'])
    c.execute("""create or replace table song_metrics as select b.*,l.* exclude(song_key),
        rank() over(partition by period_start order by listeners desc) listener_rank,
        rank() over(partition by period_start order by listens desc) listen_rank
        from song_base b join song_labels l using(song_key)""")
    c.execute("""create or replace table user_artists as select period_start,user_name,artist_mbid,
        sum(plays)::bigint plays from user_songs join song_credits using(song_key)
        group by period_start,user_name,artist_mbid""")
    metrics(c,'artist_base','user_artists',['artist_mbid'])
    c.execute("""create or replace table artist_metrics as select b.*,a.artist_name,
        rank() over(partition by period_start order by listeners desc) listener_rank,
        rank() over(partition by period_start order by listens desc) listen_rank
        from artist_base b join (select artist_mbid,min(artist_name) artist_name from song_credits group by artist_mbid) a
        using(artist_mbid)""")
    c.execute("""create or replace table artist_song_metrics as select s.*,c.artist_mbid,c.artist_name,
        s.listens::double/a.listens song_listen_share,s.listeners::double/a.listeners song_listener_reach,
        rank() over(partition by s.period_start,c.artist_mbid order by s.listeners desc) artist_song_rank
        from song_metrics s join song_credits c using(song_key)
        join artist_metrics a on s.period_start=a.period_start and c.artist_mbid=a.artist_mbid""")
    c.execute("""create or replace table coverage as select period_start,sum(plays)::bigint period_listens,
        count(distinct user_name) period_users,
        coalesce(sum(plays) filter(where linked),0)::bigint linked_listens,
        count(distinct user_name) filter(where linked) linked_users,
        coalesce(sum(plays) filter(where linked),0)::double/sum(plays) artist_coverage
        from (select *,song_key in(select song_key from song_credits) linked from user_songs) group by period_start""")
    c.execute("""create or replace table user_variants as select period_start,user_name,variant,
        sum(plays)::bigint plays from user_songs join song_labels using(song_key)
        group by period_start,user_name,variant""")
    metrics(c,'variant_base','user_variants',['variant'])
    c.execute("""create or replace table variant_metrics as select b.*,
        listens::double/period_listens listen_share,listeners::double/period_users listener_reach
        from variant_base b join coverage using(period_start)""")
    c.execute("""create or replace table listener_metrics as select period_start,sha256(user_name) listener_key,
        sum(plays)::bigint listens,count(*) song_count,max(plays)::double/sum(plays) top_song_share
        from user_songs group by period_start,user_name""")
    c.execute("""create or replace table identity_coverage as select period_start,identity_status,
        sum(plays)::bigint listens,count(distinct user_name) listeners from user_songs join song_labels using(song_key)
        group by period_start,identity_status""")
    total=c.execute('select sum(play_count) from monthly').fetchone()[0]
    for table,field in [('song_metrics','listens'),('coverage','period_listens'),
                        ('variant_metrics','listens'),('listener_metrics','listens'),('identity_coverage','listens')]:
        if c.execute(f'select sum({field}) from {table}').fetchone()[0]!=total:
            raise ValueError('count mismatch: '+table)
    if c.execute('select count(*) from artist_metrics where listeners>listens or top_user_share>1 or top_user_share<=0').fetchone()[0]:
        raise ValueError('invalid artist metric')
