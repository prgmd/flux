import pytest

from flux.validation.ci_scope import event_base,select_engines


@pytest.mark.parametrize('paths,expected',[
    (['README.md'],{'dbt':False,'gx':False}),
    (['dbt/flux_gold/models/marts/yearly_song_metrics.sql'],{'dbt':True,'gx':False}),
    (['src/flux/validation/gold_changes.py'],{'dbt':True,'gx':True}),
    (['tests/test_dbt_increment.py'],{'dbt':True,'gx':True}),
    (['scripts/update_dbt_gold.py'],{'dbt':True,'gx':True}),
    (['requirements-gx.txt'],{'dbt':False,'gx':True}),
    (['requirements-dbt.txt'],{'dbt':True,'gx':False}),
    (['requirements-dev.txt'],{'dbt':True,'gx':True}),
])
def test_changed_code_and_dependencies_select_required_engines(paths,expected):
    assert select_engines(paths)==expected


def test_manual_force_and_workflow_changes_cannot_skip_integrations():
    assert select_engines([],force=True)=={'dbt':True,'gx':True}
    assert select_engines(['.github/workflows/data-contracts.yml'])=={'dbt':True,'gx':True}


def test_event_base_uses_pull_request_target_or_push_before_and_handles_initial_branch():
    assert event_base('pull_request',{'pull_request':{'base':{'sha':'target'}}})=='target'
    assert event_base('push',{'before':'old'})=='old'
    assert event_base('push',{'before':'0'*40}) is None
    assert event_base('workflow_dispatch',{}) is None
