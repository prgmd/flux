import json
from pathlib import Path

import pytest

pytest.importorskip('dbt.cli.main')

from test_dbt_pilot import baseline
from flux.validation.dbt_pilot import bootstrap,invoke

pytestmark=pytest.mark.dbt


@pytest.mark.parametrize('variables',[
    {'flux_incremental':True,'flux_months':["2026-01'); delete from pinned.monthly; --"],'flux_years':['2026']},
    {'flux_incremental':True,'flux_months':['2026-01']},
    {'flux_incremental':'yes','flux_months':['2026-01'],'flux_years':['2026']},
])
def test_invalid_or_missing_scope_cannot_compile_into_period_deletion(baseline,tmp_path,variables):
    root=tmp_path/'candidate';bootstrap(baseline,root)
    project=Path(__file__).resolve().parents[1]/'dbt/flux_gold'
    result=invoke(root,project,['compile','--vars',json.dumps(variables)],tag='invalid-periods')
    assert result['returncode']!=0
