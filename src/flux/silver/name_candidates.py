import unicodedata

from flux.silver.name_character_catalog import policy


POLICY_VERSION = 'sample-name-v2-latin-accents'
REVIEWED_ARTIST_ALIASES = policy()['reviewed_artist_aliases']
CHARACTERS = str.maketrans(policy()['approved_characters'])


def remove_latin_accents(value):
    result=[]
    previous_latin=False
    for char in unicodedata.normalize('NFD',value):
        if unicodedata.category(char).startswith('M'):
            if not (previous_latin and unicodedata.combining(char)):
                result.append(char)
        else:
            result.append(char)
            previous_latin='LATIN' in unicodedata.name(char,'')
    return unicodedata.normalize('NFC',''.join(result))


NORMALIZATION_STEPS = (
    ('NFC',lambda v:unicodedata.normalize('NFC',v)),
    ('lower',str.lower),
    ('latin_accents',remove_latin_accents),
    ('approved_characters',lambda v:v.translate(CHARACTERS)),
    ('whitespace',lambda v:' '.join(v.split())),
)


def _normalize(value,steps):
    if not isinstance(value,str):
        raise ValueError('name must be a nonempty string')
    normalized=value
    for _,transform in steps:
        normalized=transform(normalized)
    if not normalized:
        raise ValueError('name must be a nonempty string')
    return normalized


def normalize_text(value):
    return _normalize(value,NORMALIZATION_STEPS)


def artist_alias_key(value):
    spelling=_normalize(value,(step for step in NORMALIZATION_STEPS if step[0]!='latin_accents'))
    return REVIEWED_ARTIST_ALIASES.get(spelling)


def candidate_key(artist,title):
    normalized=normalize_text(artist)
    artist_id=artist_alias_key(artist)
    return ('artist_mbid' if artist_id else 'artist_name',artist_id or normalized,normalize_text(title))
