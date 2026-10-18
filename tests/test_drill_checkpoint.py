import json

import pytest

from flux.common.drill_checkpoint import atomic_save, recovered_merge


def test_no_commit_needs_execution():
    assert recovered_merge([], 1, "run-a") is None


def test_commit_after_crash_is_recovered():
    history = [{"version": 2, "userMetadata": "run-a", "operation": "MERGE",
                "operationMetrics": {"numTargetRowsInserted": "5"}}]
    assert recovered_merge(history, 1, "run-a") == {
        "version": 2, "metrics": {"numTargetRowsInserted": 5}}


@pytest.mark.parametrize("history", [
    [{"version": 2, "userMetadata": "another", "operation": "MERGE"}],
    [{"version": 3, "userMetadata": "run-a", "operation": "MERGE"}],
    [{"version": 2, "userMetadata": "run-a", "operation": "WRITE"}],
    [{"version": 2, "userMetadata": "run-a", "operation": "MERGE"},
     {"version": 3, "userMetadata": "run-a", "operation": "MERGE"}],
])
def test_unexpected_commits_reject_resume(history):
    with pytest.raises(RuntimeError):
        recovered_merge(history, 1, "run-a")


def test_older_commits_do_not_count_as_new():
    assert recovered_merge([{"version": 1}], 1, "run-a") is None


def test_atomic_report_is_readable_and_replaced(tmp_path):
    path = tmp_path / "result.json"
    atomic_save(path, {"stage": 1})
    atomic_save(path, {"stage": 2})
    assert json.loads(path.read_text(encoding="utf-8")) == {"stage": 2}
    assert not path.with_suffix(".tmp").exists()
