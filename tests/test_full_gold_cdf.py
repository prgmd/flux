import duckdb
import pytest

from flux.validation.full_gold_cdf import apply_cdf, compare_relations, compatible_models, consume_cdf


def setup(c):
    c.execute('create schema pinned')
    c.execute("create table pinned.monthly(user_name varchar,recording_msid varchar,listened_month varchar,play_count bigint)")
    c.execute("insert into pinned.monthly values ('u','s','2025-01',2),('v','s','2025-02',1)")
    c.execute('create table feed as select *,cast(null as varchar) as _change_type,cast(null as bigint) as _commit_version from pinned.monthly where false')


def test_sql_cdf_replaces_totals_and_moves_keys_transactionally():
    with duckdb.connect() as c:
        setup(c)
        c.execute("insert into feed values ('u','s','2025-01',2,'update_preimage',1),('u','s','2026-02',4,'update_postimage',1),('v','s','2025-02',1,'delete',2),('w','s','2026-02',1,'insert',2)")
        result=apply_cdf(c,'feed',0,2)
        assert result['months']==['2025-01','2025-02','2026-02']
        assert result['years']==['2025','2026']
        assert c.execute('select * from pinned.monthly order by user_name').fetchall()==[('u','s','2026-02',4),('w','s','2026-02',1)]


@pytest.mark.parametrize('values',[
    "('u','s','2025-01',9,'update_preimage',1),('u','s','2025-01',10,'update_postimage',1)",
    "('u','s','2025-01',2,'insert',1)",
    "('u','s','2025-01',2,'delete',0)",
    "('u','s','2025-01',2,'bad',1)",
    "('u','s','2025-01',0,'update_postimage',1)",
    "('u','s','2025-01',2,'delete',1),('u','s','2025-01',2,'delete',1)",
])
def test_bad_feed_rolls_back_entire_range(values):
    with duckdb.connect() as c:
        setup(c)
        before=c.execute('select * from pinned.monthly order by user_name').fetchall()
        c.execute('insert into feed values '+values)
        with pytest.raises(ValueError):apply_cdf(c,'feed',0,1)
        assert before==c.execute('select * from pinned.monthly order by user_name').fetchall()


def test_later_bad_commit_rolls_back_previous_commits():
    with duckdb.connect() as c:
        setup(c)
        c.execute("insert into feed values ('v','s','2025-02',1,'delete',1),('x','s','2025-01',1,'delete',2)")
        with pytest.raises(ValueError):apply_cdf(c,'feed',0,2)
        assert c.execute('select count(*) from pinned.monthly').fetchone()[0]==2


def test_successive_commits_replace_the_same_key_in_version_order():
    with duckdb.connect() as c:
        setup(c)
        c.execute("insert into feed values ('u','s','2025-01',3,'update_preimage',2),('u','s','2025-01',4,'update_postimage',2),('u','s','2025-01',2,'update_preimage',1),('u','s','2025-01',3,'update_postimage',1)")
        result=apply_cdf(c,'feed',0,2)
        assert result['commits']==2 and result['cdf_rows']==4
        assert c.execute('select sum(play_count) from pinned.monthly').fetchone()[0]==5


def test_full_comparison_detects_removed_period_and_untouched_corruption():
    with duckdb.connect() as c:
        setup(c)
        c.execute('create table expected as select * from pinned.monthly')
        found=compare_relations(c,'pinned.monthly','expected',['user_name','recording_msid','listened_month'],'listened_month')
        assert found['matches'] and found['rows']==2
        c.execute("delete from pinned.monthly where listened_month='2025-01'")
        c.execute("update pinned.monthly set play_count=3 where listened_month='2025-02'")
        found=compare_relations(c,'pinned.monthly','expected',['user_name','recording_msid','listened_month'],'listened_month')
        assert not found['matches'] and found['missing']==1 and found['columns']['play_count']==1


def test_empty_range_does_not_add_counts():
    with duckdb.connect() as c:
        setup(c)
        assert apply_cdf(c,'feed',2,2)=={'months':[],'years':[],'cdf_rows':0,'commits':0}
        assert c.execute('select sum(play_count) from pinned.monthly').fetchone()[0]==3


def test_consumed_range_replay_and_unknown_cursor():
    with duckdb.connect() as c:
        setup(c)
        c.execute("insert into feed values ('w','s','2025-01',3,'insert',2)")
        assert consume_cdf(c,'feed',0,2,0)['cdf_rows']==1
        assert consume_cdf(c,'feed',0,2,2)['cdf_rows']==0
        assert c.execute('select sum(play_count) from pinned.monthly').fetchone()[0]==6
        for cursor in [-1,1,3]:
            with pytest.raises(ValueError,match='cursor'):consume_cdf(c,'feed',0,2,cursor)


def test_baseline_upgrade_allows_only_period_wrapper_and_descriptions(tmp_path):
    old,new=tmp_path/'old',tmp_path/'new'
    for root in [old,new]:(root/'models/marts').mkdir(parents=True)
    name='models/marts/monthly_song_metrics.sql'
    (old/name).write_text('select 1 as value\n',encoding='utf-8')
    (new/name).write_text("{{ flux_period_config('month') }}\nselect * from (\nselect 1 as value\n) as period_snapshot\n{% if var('flux_incremental', false) %}\nwhere period_start in ({{ flux_dates_sql('month') }})\n{% endif %}\n",encoding='utf-8')
    assert compatible_models(old,new)==[name]
    (new/name).write_text((new/name).read_text().replace('select 1 as','select 2 as'),encoding='utf-8')
    with pytest.raises(ValueError,match='contract'):compatible_models(old,new)
