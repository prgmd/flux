import importlib.util
from pathlib import Path

import pytest

duckdb = pytest.importorskip('duckdb')
spec = importlib.util.spec_from_file_location('catalog', Path(__file__).resolve().parents[1] / 'scripts/investigations/catalog_title_variants.py')
catalog = importlib.util.module_from_spec(spec)
spec.loader.exec_module(catalog)


def test_suffix_candidates_preserve_originals_and_version_risks():
    with duckdb.connect() as c:
        c.execute("""create table metadata as select * from (values
        ('a','Artist','Song','r'),('b','Artist','Song (Sped Up)','speed'),
        ('c','Artist','Song - Slowed + Reverb','slow'),('d','Artist','Live Forever','live'),
        ('e','Artist','Song [Explicit]','r'),('f','Other','Song (Sped Up)','other'),
        ('g','Artist','Song (Are We There)','unknown'),('h','Artist','Song (Live) [Remastered]','live2'),
        ('i','Artist','別名','r'),('a','Artist','Song','r'))
        t(recording_msid,artist_name,track_name,recording_mbid)""")
        catalog.build(c)
        assert c.execute("select count(*) from title_rows").fetchone()[0] == 9
        assert c.execute("select base_title,category from title_rows where recording_msid='b'").fetchone() == ('song','speed')
        assert c.execute("select category from title_rows where recording_msid='c'").fetchone()[0] == 'speed'
        assert c.execute("select base_title from title_rows where recording_msid='d'").fetchone()[0] == 'live forever'
        assert c.execute("select category from title_rows where recording_msid='g'").fetchone()[0] == 'unclassified'
        assert c.execute("select base_title from title_rows where recording_msid='h'").fetchone()[0] == 'song (live)'
        assert c.execute("select count(*) from title_candidates where artist='other'").fetchone()[0] == 0
        assert c.execute("select count(*) from title_candidates where artist='artist'").fetchone()[0] == 1
        assert c.execute("select title_count from mbid_candidates where recording_mbid='r'").fetchone()[0] == 3
        assert c.execute("select track_name from title_rows where recording_msid='b'").fetchone()[0] == 'Song (Sped Up)'


def test_empty_or_missing_names_do_not_form_candidates():
    with duckdb.connect() as c:
        c.execute("""create table metadata as select * from (values
        ('a',NULL,'Song','r'),('b','','Song (Live)','r'),('c','Artist',NULL,'r'))
        t(recording_msid,artist_name,track_name,recording_mbid)""")
        catalog.build(c)
        assert c.execute('select count(*) from title_rows').fetchone()[0] == 0
        assert c.execute('select count(*) from title_candidates').fetchone()[0] == 0
