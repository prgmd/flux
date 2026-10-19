import importlib.util
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")
spec = importlib.util.spec_from_file_location("verify_monthly", Path(__file__).resolve().parents[1] /
                                             "scripts/migrations/verify_monthly_silver.py")
verification = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verification)


def test_합계가_같아도_키별_횟수가_틀리면_찾는다():
    c = duckdb.connect()
    c.execute("create view detail as select * from (values ('u','a','2026-01'),('u','a','2026-01'),('u','b','2026-01')) t(user_name,recording_msid,listened_month)")
    c.execute("create view actual as select * from (values ('u','a','2026-01',1),('u','b','2026-01',2)) t(user_name,recording_msid,listened_month,play_count)")
    result = verification.compare_counts(c)
    assert result["source_rows"] == result["plays"] == 3
    assert result["mismatches"] == 2


def test_정답은_통과하고_중복키는_검출한다():
    c = duckdb.connect()
    c.execute("create view detail as select 'u' user_name,'a' recording_msid,'2026-01' listened_month")
    c.execute("create view actual as select *, 1::bigint play_count from detail")
    assert verification.compare_counts(c)["mismatches"] == 0
    c.execute("create or replace view actual as select *, 1::bigint play_count from detail union all select *,1::bigint from detail")
    assert verification.compare_counts(c)["duplicate_keys"] == 1


def test_메타데이터는_원본_반복을_묶고_NULL과_배열_및_출력_중복을_대조한다():
    c = duckdb.connect()
    c.execute("create table detail as select 's' recording_msid,'Song' track_name,'Artist' artist_name,NULL::varchar recording_mbid,['a']::varchar[] artist_mbids,'NONE' mbid_source")
    c.execute("insert into detail select * from detail")
    c.execute("create table metadata as select distinct * from detail")
    assert verification.compare_metadata(c) == 0
    c.execute("insert into metadata select * from metadata")
    assert verification.compare_metadata(c) == 1
