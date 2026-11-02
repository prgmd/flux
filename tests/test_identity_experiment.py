import pytest

duckdb = pytest.importorskip('duckdb')
from flux.silver.identity_experiment import compare, register


def test_pair_metrics_capture_false_merges_and_splits():
    with duckdb.connect() as c:
        c.execute("""create table observations as select * from (values
            ('s1','main','Song','r1'),('s2','main',' song ','r1'),
            ('s3','main','Song','r2'),('s4','main','Song (Sped Up)','r3'),
            ('s5','main','Song (Remix)','r4'),('s5','main','Song (Remix)','r5'))
            t(recording_msid,artist,title,reference_mbid)""")
        register(c)
        msid=compare(c,'msid')
        basic=compare(c,'name_basic')
        assert (msid['true_pairs'],msid['predicted_pairs'],msid['correct_pairs'])==(1,1,0)
        assert (basic['true_pairs'],basic['predicted_pairs'],basic['correct_pairs'])==(1,4,1)
        assert basic['pair_precision']==pytest.approx(.25)
        assert basic['pair_recall']==1
        assert basic['conflicting_groups']==2
        with pytest.raises(ValueError):compare(c,'unknown')


def test_typography_preserves_versions_and_handles_unicode():
    with duckdb.connect() as c:
        register(c)
        c.execute("""create table observations as select * from (values
            ('s1','alt‐J','Don’t  Go','r'),('s2','alt-j','don''t go','r'),
            ('s3','alt-j','Don''t Go (Sped Up)','speed'),
            ('s4','alt-j','Don''t Go (Remix)','mix'))
            t(recording_msid,artist,title,reference_mbid)""")
        assert compare(c,'name_basic')['correct_pairs']==0
        result=compare(c,'name_typography')
        assert result['correct_pairs']==1 and result['predicted_pairs']==1
        assert result['pair_precision']==1 and result['pair_recall']==1
        assert c.execute("select name_clean('Café')=name_clean('Café')").fetchone()[0]


def test_no_pairs_are_undefined_not_perfect():
    with duckdb.connect() as c:
        register(c)
        c.execute("create table observations as select 's' recording_msid,'a' artist,'t' title,'r' reference_mbid")
        result=compare(c,'msid')
        assert result['pair_precision'] is None and result['pair_recall'] is None
