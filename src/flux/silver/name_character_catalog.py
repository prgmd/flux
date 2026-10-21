import json
import unicodedata as ud
from functools import lru_cache
from importlib.resources import files


@lru_cache(maxsize=1)
def policy():
    return json.loads(files('flux.silver').joinpath('name_matching_rules.json').read_text(encoding='utf-8'))


def load_ranges(path, script):
    found=set()
    for line in path.read_text(encoding='utf-8').splitlines():
        text=line.split('#',1)[0].strip()
        if not text:
            continue
        span,names=(x.strip() for x in text.split(';',1))
        if script not in names.split():
            continue
        edges=span.split('..')
        found.update(range(int(edges[0],16),int(edges[-1],16)+1))
    return found


def latin_candidate(value):
    if not any('LATIN' in ud.name(c,'') for c in value):
        return None
    base=''.join(c for c in ud.normalize('NFD',value.casefold()) if ud.category(c)!='Mn')
    base=base.translate(str.maketrans(policy()['review_latin_transliterations']))
    return base if base and all('a'<=c<='z' for c in base) else None


def describe(char, *, latin=False, confusable=False):
    if len(char)!=1:
        raise ValueError('one code point required')
    category=ud.category(char)
    if category in ('Cn','Cs','Co'):
        return None
    nfc=ud.normalize('NFC',char)
    nfkc=ud.normalize('NFKC',char)
    lower,upper,fold=char.lower(),char.upper(),char.casefold()
    official_latin=latin
    latin=latin or 'LATIN' in ud.name(char,'')
    if not (latin or confusable or category[0] in 'MPSNZ' or category in ('Cf','Cc')
            or lower!=char or upper!=char or fold!=char or nfc!=char or nfkc!=char):
        return None
    approved=policy()['approved_characters'].get(char)
    if char.isspace():
        action='whitespace'
    elif approved is not None:
        action='approved_character'
    elif category in ('Cf','Cc'):
        action='review_invisible'
    elif category.startswith('M'):
        action='preserve_mark'
    elif category.startswith('P'):
        action='preserve_punctuation'
    elif latin:
        action='review_latin_variants'
    else:
        action='preserve_review'
    return {'character':char,'codepoint':f'U+{ord(char):04X}','name':ud.name(char,''),
            'category':category,'latin_script_or_extension':official_latin,
            'latin_named_or_script':latin,'action':action,
            'approved_replacement':approved,'nfc':nfc,'nfd':ud.normalize('NFD',char),
            'nfkc':nfkc,'nfkd':ud.normalize('NFKD',char),'lower':lower,'upper':upper,
            'casefold':fold,'latin_candidate':latin_candidate(char) if latin else None,
            'confusable_source':confusable,'unicode_version':ud.unidata_version}
