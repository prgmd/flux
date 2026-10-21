import unicodedata as ud
from collections import defaultdict
from itertools import combinations

from flux.silver.name_candidates import NORMALIZATION_STEPS, artist_alias_key, candidate_key, normalize_text
from flux.silver.name_character_catalog import latin_candidate


MODES = ('approved','nfkc','casefold','latin_fold')


def latin_text(value):
    result=[];previous_latin=False
    for char in ud.normalize('NFC',value):
        if 'LATIN' in ud.name(char,''):
            result.append(latin_candidate(char) or char)
            previous_latin=True
        elif ud.category(char)=='Mn' and previous_latin:
            continue
        else:
            result.append(char)
            previous_latin=False
    return ''.join(result)


def fields(artist,title):
    return {'artist':normalize_text(artist),'title':normalize_text(title),
            'artist_alias_mbid':artist_alias_key(artist),'candidate_key':candidate_key(artist,title)}


def preview(row):
    artist,title=row['artist_name'],row['track_name']
    changes=[]
    for field,value in (('artist',artist),('title',title)):
        for name,transform in NORMALIZATION_STEPS:
            after=transform(value)
            if value!=after:
                changes.append({'field':field,'rule':name,'before':value,'after':after})
            value=after
    return {'original':dict(row),'approved':fields(artist,title),'changes':changes,
            'review_only':{
                'nfkc':fields(ud.normalize('NFKC',artist),ud.normalize('NFKC',title)),
                'casefold':fields(artist.casefold(),title.casefold()),
                'latin_fold':fields(latin_text(artist),latin_text(title))}}


def review_groups(rows,mode):
    if mode not in MODES:
        raise ValueError(mode)
    groups=defaultdict(list)
    for index,row in enumerate(rows):
        fields=row['approved'] if mode=='approved' else row['review_only'][mode]
        groups[tuple(fields['candidate_key'])].append((index,row))
    result=[]
    for key,members in groups.items():
        if len(members)<2:
            continue
        refs={r['original'].get('resolved_mbid') for _,r in members}
        unknown=None in refs
        refs.discard(None)
        verdict=('reference_conflict' if len(refs)>1 else 'reference_missing' if not refs else
                 'same_reference_with_unverified' if unknown else 'same_reference_candidate')
        result.append({'key':key,'rows':[i for i,_ in members],
                       'raw_name_pairs':len({(r['original']['artist_name'],r['original']['track_name']) for _,r in members}),
                       'reference_recordings':sorted(refs),'unknown_reference':unknown,'verdict':verdict})
    return result


def compare_baseline(rows,baseline):
    if [r['original'] for r in rows]!=[r['original'] for r in baseline]:
        raise ValueError('baseline must contain the same sample in the same order')
    def text_key(record):
        return (record['approved']['artist'],record['approved']['title'])
    def pairs(records,text_only=False):
        groups=defaultdict(list)
        for index,record in enumerate(records):
            key=text_key(record) if text_only else tuple(record['approved']['candidate_key'])
            groups[key].append(index)
        return {pair for indices in groups.values() for pair in combinations(indices,2)}
    old_pairs,new_pairs=pairs(baseline),pairs(rows)
    old_text_pairs,new_text_pairs=pairs(baseline,True),pairs(rows,True)
    added=sorted(new_pairs-old_pairs)
    added_text=sorted(new_text_pairs-old_text_pairs)
    def conflicts(added_pairs):
        result=[]
        for left,right in added_pairs:
            refs=[rows[i]['original'].get('resolved_mbid') for i in (left,right)]
            if all(refs) and refs[0]!=refs[1]:
                result.append([left,right])
        return result
    changes=[]
    for index,(current,old) in enumerate(zip(rows,baseline)):
        old_fields={**old['approved'],'candidate_key':tuple(old['approved']['candidate_key'])}
        current_fields={**current['approved'],'candidate_key':tuple(current['approved']['candidate_key'])}
        if current_fields!=old_fields:
            changes.append({'row':index,'before':old['approved'],'after':current['approved']})
    return {'sample_rows':len(rows),'changed_artist_rows':sum(c['before']['artist']!=c['after']['artist'] for c in changes),
            'changed_title_rows':sum(c['before']['title']!=c['after']['title'] for c in changes),
            'changed_candidate_key_rows':sum(tuple(c['before']['candidate_key'])!=tuple(c['after']['candidate_key']) for c in changes),
            'newly_equal_pairs':[list(p) for p in added],
            'no_longer_equal_pairs':[list(p) for p in sorted(old_pairs-new_pairs)],
            'new_reference_conflict_pairs':conflicts(added),
            'text_key_count_before':len({text_key(r) for r in baseline}),
            'text_key_count_after':len({text_key(r) for r in rows}),
            'newly_equal_text_pairs':[list(p) for p in added_text],
            'no_longer_equal_text_pairs':[list(p) for p in sorted(old_text_pairs-new_text_pairs)],
            'new_text_reference_conflict_pairs':conflicts(added_text),'changed_rows':changes}
