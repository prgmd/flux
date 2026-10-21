import pytest

from flux.silver.name_candidates import artist_alias_key, candidate_key, normalize_text


def test_unicode_forms_and_reviewed_artist_alias():
    artist_id='859d0860-d480-4efd-970c-c05d5f1776b8'
    assert normalize_text('Beyonce\u0301')==normalize_text('Beyoncé')
    assert normalize_text('Beyonce')==normalize_text('Beyoncé')=='beyonce'
    assert artist_alias_key('Beyonce')==artist_alias_key('Beyoncé')==artist_id
    assert candidate_key('Beyonce','Irreplaceable')==candidate_key('Beyoncé','Irreplaceable')


def test_versions_are_retained_while_comparison_accents_are_removed():
    assert candidate_key('a','Song')!=candidate_key('a','Song (Sped Up)')
    assert candidate_key('a','Song (Remix)')!=candidate_key('a','Song (Sped Up)')
    assert candidate_key('a','Song (Clean)')!=candidate_key('a','Song (Explicit)')
    assert candidate_key('Resume','Song')==candidate_key('Résumé','Song')
    assert candidate_key('a','Cafe')==candidate_key('a','Café')
    assert normalize_text('  alt‐J\t  ')=='alt-j'
    assert normalize_text('Don’t\u00a0 Go')=="don't go"
    assert candidate_key('ab','c')!=candidate_key('a','bc')


@pytest.mark.parametrize('value,expected',[
    ('É È Ê Ë Ē Ě Ẽ Ę Ñ Ç Ğ Ș','e e e e e e e e n c g s'),
    ('Beyonce\u0301','beyonce'),('İ I ı','i i ı'),
    ('Phœnix ø æ ł ß ﬁ','phœnix ø æ ł ß ﬁ'),
    ('가 ㅠㅠ ά й أ ❤\ufe0f 👩\u200d🎤','가 ㅠㅠ ά й أ ❤\ufe0f 👩\u200d🎤'),
    ('e\ufe0f e\u034f e\u200d\u0301 \u0301e','e\ufe0f e\u034f e\u200d\u0301 \u0301e'),
])
def test_accent_scope_preserves_transliterations_other_scripts_and_formatting(value,expected):
    assert normalize_text(value)==expected
    assert normalize_text(normalize_text(value))==expected


def test_missing_names_are_not_merged():
    with pytest.raises(ValueError):candidate_key(None,'Song')
    with pytest.raises(ValueError):candidate_key('a','  ')


def test_accent_removal_does_not_authorize_new_official_artist_aliases():
    assert normalize_text('Beyoncè')=='beyonce'
    assert artist_alias_key('Beyoncè') is None
    assert candidate_key('Beyoncè','Song')[0]=='artist_name'
    assert candidate_key('Beyonce','Song')[0]=='artist_mbid'
