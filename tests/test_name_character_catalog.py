import unicodedata

import pytest

from flux.silver.name_character_catalog import describe, latin_candidate, load_ranges, policy


def test_latin_case_accent_and_compatibility_candidates():
    assert latin_candidate('é')=='e'
    assert latin_candidate('ø')=='o'
    assert latin_candidate('æ')=='ae'
    assert describe('ß',latin=True)['casefold']=='ss'
    assert describe('İ',latin=True)['lower']=='i\u0307'
    assert describe('Ａ',latin=True)['nfkc']=='A'
    assert describe('①')['nfkc']=='1'
    assert describe('K')['nfc']=='K'


def test_special_characters_are_classified_without_deleting_them():
    assert describe('\u00a0')['action']=='whitespace'
    assert describe('’')['approved_replacement']=="'"
    assert describe('‐')['approved_replacement']=='-'
    assert describe('—')['approved_replacement'] is None
    assert describe('\u200b')['action']=='review_invisible'
    assert describe('\u200d')['action']=='review_invisible'
    assert describe('\ufe0f')['action']=='preserve_mark'
    assert describe('&')['action']=='preserve_punctuation'
    assert describe('ο')['latin_candidate'] is None
    assert describe('а')['latin_candidate'] is None
    assert describe('가') is None


def test_unicode_range_parser_and_policy_contract(tmp_path):
    p=tmp_path/'Scripts.txt'
    p.write_text('0041..005A ; Latin # letters\n0061 ; Latin\n03B1 ; Greek\n')
    assert load_ranges(p,'Latin')==set(range(65,91))|{97}
    config=policy()
    assert config['policy_version']=='name-preprocessing-registry-v2'
    accents=next(f for f in config['families'] if f['id']=='latin_diacritics')
    assert accents['action']=='remove_latin_accents' and accents['scope']=='comparison_only'
    assert config['unicode_version']==unicodedata.unidata_version
    assert config['default_action']=='preserve'
    assert config['production_applied'] is False
    with pytest.raises(ValueError):describe('two')


def test_normalization_suite_runner_detects_wrong_expectations(tmp_path):
    import importlib.util
    from pathlib import Path
    spec=importlib.util.spec_from_file_location('catalog_build',Path(__file__).resolve().parents[1]/'scripts/investigations/build_name_character_catalog.py')
    driver=importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
    p=tmp_path/'NormalizationTest.txt'
    p.write_text('00E9;00E9;0065 0301;00E9;0065 0301; # e acute\n',encoding='utf-8')
    assert driver.verify_normalization(p)=={'cases':1,'assertions':20,'passed':True}
    p.write_text('00E9;0065;0065;0065;0065;\n')
    with pytest.raises(ValueError,match='failure'):driver.verify_normalization(p)


def test_catalog_build_is_complete_for_declared_source_and_never_overwrites(tmp_path):
    import importlib.util
    import json
    from pathlib import Path
    spec=importlib.util.spec_from_file_location('catalog_driver',Path(__file__).resolve().parents[1]/'scripts/investigations/build_name_character_catalog.py')
    driver=importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
    source=tmp_path/'sources';source.mkdir()
    files={'Scripts.txt':'0041 ; Latin\n','ScriptExtensions.txt':'0061 ; Latn\n',
           'NormalizationTest.txt':'00E9;00E9;0065 0301;00E9;0065 0301;\n',
           'confusables.txt':'0391 ; 0041 ; MA\n'}
    for name,body in files.items():
        (source/name).write_text('# 16.0.0\n'+body,encoding='utf-8')
    output=tmp_path/'out';result=driver.build(source,output)
    assert result['completed'] and result['latin_script_or_extension_codepoints']==2
    assert result['confusable_mappings']==1 and result['scalar_values_examined']==1112064
    assert result['production_applied'] is False and result['music_corpus_scanned'] is False
    assert json.loads((output/'confusables.json').read_text(encoding='utf-8'))[0]['action']=='review_only'
    with pytest.raises(FileExistsError):driver.build(source,output)
    (source/'Scripts.txt').write_text('# 17.0.0\n0041 ; Latin\n')
    with pytest.raises(ValueError,match='version'):driver.build(source,tmp_path/'bad')
