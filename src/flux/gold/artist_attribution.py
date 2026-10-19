def classify(c):
    c.execute("""create or replace temp macro typography(v) as
        translate(lower(trim(v)), '‘’‐‑', '''''--')""")
    c.execute("""create or replace table reviewed as
        with matched as (select p.*,x.resolved_mbid,
            r.recording_title resolved_title,r.comment resolved_comment,r.artist_credit resolved_credit,
            exists(select 1 from credits a where a.artist_credit=r.artist_credit and
                typography(p.artist) in (typography(a.artist_name),typography(a.credit_name),typography(a.credit_display))) artist_matches
            from prior p left join resolution x on p.recording_mbid=x.original_mbid
            left join recordings r on x.resolved_mbid=r.recording_mbid)
        select * exclude(artist_matches),case
            when mbids=0 then 'no_mbid' when mbids>1 then 'mbid_conflict'
            when status='name_conflict' then 'name_conflict'
            when resolved_title is null then 'official_missing'
            when typography(title)<>typography(resolved_title) then 'title_conflict'
            when not artist_matches then 'artist_conflict'
            else 'accepted' end artist_status
        from matched""")
    c.execute("""create or replace table proposed_credits as select distinct
        recording_msid,recording_mbid,resolved_mbid,a.artist_mbid,a.artist_name
        from reviewed r join credits a on r.resolved_credit=a.artist_credit
        where artist_status='accepted'""")
