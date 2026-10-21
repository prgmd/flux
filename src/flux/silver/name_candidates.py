import unicodedata


POLICY_VERSION = 'sample-name-v1'
REVIEWED_ARTIST_ALIASES = {
    'beyonce': '859d0860-d480-4efd-970c-c05d5f1776b8',
    'beyoncé': '859d0860-d480-4efd-970c-c05d5f1776b8',
}
CHARACTERS = str.maketrans({'‘': "'", '’': "'", '‐': '-', '‑': '-'})


def normalize_text(value):
    if not isinstance(value,str):
        raise ValueError('name must be a nonempty string')
    normalized=' '.join(unicodedata.normalize('NFC',value).lower().translate(CHARACTERS).split())
    if not normalized:
        raise ValueError('name must be a nonempty string')
    return normalized


def artist_alias_key(value):
    return REVIEWED_ARTIST_ALIASES.get(normalize_text(value))


def candidate_key(artist,title):
    normalized=normalize_text(artist)
    artist_id=REVIEWED_ARTIST_ALIASES.get(normalized)
    return ('artist_mbid' if artist_id else 'artist_name',artist_id or normalized,normalize_text(title))
