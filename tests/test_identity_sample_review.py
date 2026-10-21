import pytest

from flux.silver.name_candidates import artist_alias_key, candidate_key, normalize_text


def test_unicode_forms_and_reviewed_artist_alias():
    artist_id='859d0860-d480-4efd-970c-c05d5f1776b8'
    assert normalize_text('Beyonce\u0301')==normalize_text('Beyoncé')
    assert normalize_text('Beyonce')!=normalize_text('Beyoncé')
    assert artist_alias_key('Beyonce')==artist_alias_key('Beyoncé')==artist_id
    assert candidate_key('Beyonce','Irreplaceable')==candidate_key('Beyoncé','Irreplaceable')


def test_versions_and_unreviewed_accents_are_not_removed():
    assert candidate_key('a','Song')!=candidate_key('a','Song (Sped Up)')
    assert candidate_key('a','Song (Remix)')!=candidate_key('a','Song (Sped Up)')
    assert candidate_key('a','Song (Clean)')!=candidate_key('a','Song (Explicit)')
    assert candidate_key('Resume','Song')!=candidate_key('Résumé','Song')
    assert candidate_key('a','Cafe')!=candidate_key('a','Café')
    assert normalize_text('  alt‐J\t  ')=='alt-j'
    assert normalize_text('Don’t\u00a0 Go')=="don't go"
    assert candidate_key('ab','c')!=candidate_key('a','bc')


def test_missing_names_are_not_merged():
    with pytest.raises(ValueError):candidate_key(None,'Song')
    with pytest.raises(ValueError):candidate_key('a','  ')
