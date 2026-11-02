POLICIES = {
    'msid': 'recording_msid',
    'name_basic': 'to_json(struct_pack(artist:=lower(trim(artist)),track:=lower(trim(title))))',
    'name_typography': 'to_json(struct_pack(artist:=name_clean(artist),track:=name_clean(title)))',
}


def register(c):
    c.execute("""create or replace temp macro name_clean(v) as
        trim(regexp_replace(translate(lower(nfc_normalize(v)), '‘’‐‑', '''''--'),
            '[\\s\\p{Z}]+',' ','g'))""")


def compare(c, policy):
    if policy not in POLICIES:
        raise ValueError(policy)
    c.execute(f"""create or replace temp table cells as select {POLICIES[policy]} candidate,
        reference_mbid,count(*)::bigint n from observations group by 1,2""")
    correct=c.execute('select coalesce(sum(n*(n-1)//2),0) from cells').fetchone()[0]
    predicted=c.execute('select coalesce(sum(n*(n-1)//2),0) from (select sum(n)::bigint n from cells group by candidate)').fetchone()[0]
    truth=c.execute('select coalesce(sum(n*(n-1)//2),0) from (select sum(n)::bigint n from cells group by reference_mbid)').fetchone()[0]
    groups,conflicts,conflict_rows=c.execute("""select count(*),count(*) filter(where refs>1),
        coalesce(sum(n) filter(where refs>1),0) from
        (select candidate,count(*) refs,sum(n) n from cells group by candidate)""").fetchone()
    splits=c.execute('select count(*) from (select reference_mbid from cells group by reference_mbid having count(*)>1)').fetchone()[0]
    precision=correct/predicted if predicted else None
    recall=correct/truth if truth else None
    return {'observations':c.execute('select sum(n) from cells').fetchone()[0],
            'groups':groups,'conflicting_groups':conflicts,'observations_in_conflicting_groups':conflict_rows,
            'split_reference_recordings':splits,'true_pairs':truth,'predicted_pairs':predicted,
            'correct_pairs':correct,'false_merge_pairs':predicted-correct,'missed_pairs':truth-correct,
            'pair_precision':precision,'pair_recall':recall,
            'pair_f1':2*precision*recall/(precision+recall) if precision is not None and recall is not None and precision+recall else None}
