import unicodedata as ud
from collections import defaultdict

from flux.silver.name_candidates import CHARACTERS, artist_alias_key, candidate_key, normalize_text
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
    steps=(('NFC',lambda v:ud.normalize('NFC',v)),('lower',str.lower),
           ('approved_characters',lambda v:v.translate(CHARACTERS)),
           ('whitespace',lambda v:' '.join(v.split())))
    for field,value in (('artist',artist),('title',title)):
        for name,transform in steps:
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
